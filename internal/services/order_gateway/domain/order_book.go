package domain

import (
	"context"
	"fmt"
	"github.com/quickfixgo/enum"
	"github.com/shopspring/decimal"
)

type OrderBook struct {
	symbol    string
	askLevels []*bookLevel
	bidLevels []*bookLevel
}

func NewOrderBook(symbol string) *OrderBook {
	return &OrderBook{
		symbol:    symbol,
		askLevels: make([]*bookLevel, 0),
		bidLevels: make([]*bookLevel, 0),
	}
}

type bookLevel struct {
	orders                   []*Order
	px                       decimal.Decimal
	sessionAndClOrdIDtoOrder map[string]map[string]*Order
}

func newBookLevel(orders []*Order, px decimal.Decimal) *bookLevel {
	sessionAndClOrdIDtoIndexMap := make(map[string]map[string]*Order)
	level := bookLevel{px: px, sessionAndClOrdIDtoOrder: sessionAndClOrdIDtoIndexMap}
	for _, order := range orders {
		err := level.Add(order)
		if err != nil {
			panic(err)
		}
	}
	return &level
}

func (l *bookLevel) IsEmpty() bool {
	return len(l.orders) == 0
}
func (l *bookLevel) Price() decimal.Decimal {
	return l.px
}
func (l *bookLevel) Add(o *Order) error {
	sessionOrders, ok := l.sessionAndClOrdIDtoOrder[o.senderCompID]
	if !ok {
		sessionOrders = make(map[string]*Order)
		l.sessionAndClOrdIDtoOrder[o.senderCompID] = sessionOrders
	}
	if _, ok2 := sessionOrders[o.clOrdID]; !ok2 {
		sessionOrders[o.clOrdID] = o
		l.orders = append(l.orders, o)
		return nil
	} else {
		return fmt.Errorf("order %s already exists", o.clOrdID)
	}
}
func (l *bookLevel) Pop(clOrdID string) (*Order, error) {
	//pop permanently removes the message from the queue, regardless of status
	if len(l.orders) == 0 {
		return nil, fmt.Errorf("cannot pop from empty book level")
	}
	o := l.orders[0]
	if o.clOrdID != clOrdID {
		return nil, fmt.Errorf("wrong clOrdID when poping from book level, exp %s, got %s", clOrdID, o.clOrdID)
	}
	l.orders = l.orders[1:]
	sessionOrders := l.sessionAndClOrdIDtoOrder[o.senderCompID]
	delete(sessionOrders, o.clOrdID)
	if len(sessionOrders) == 0 {
		delete(l.sessionAndClOrdIDtoOrder, o.senderCompID)
	}
	return o, nil
}
func (l *bookLevel) Cancel(o *Order) error {
	//cancel just sets the order status to be garbage collected later
	sessionOrders, ok := l.sessionAndClOrdIDtoOrder[o.senderCompID]
	if !ok {
		return fmt.Errorf("order %s-%s does not exists", o.senderCompID, o.clOrdID)
	}
	if ord, ok2 := sessionOrders[o.clOrdID]; !ok2 {
		return fmt.Errorf("order %s-%s does not exists", o.senderCompID, o.clOrdID)
	} else {
		ord.status = OrderStatusCanceled
		return nil
	}

}

func (b *OrderBook) MatchOrAdd(ctx context.Context, order *Order) ([]*Order, error) {
	switch order.ordType {
	case enum.OrdType_MARKET:
		return b.matchMarketOrder(order)
	case enum.OrdType_LIMIT:
		return b.matchLimitOrder(order)
	default:
		return nil, fmt.Errorf("order type %s is not supported", order.ordType)
	}
}

func (b *OrderBook) matchMarketOrder(order *Order) ([]*Order, error) {
	matches := make([]*Order, 0)
	if order.side == BUY {
		if len(b.askLevels) == 0 {
			return nil, fmt.Errorf("no sell side orders to match market order")
		}
		for len(b.askLevels) > 0 {
			level := b.askLevels[0]
			levelMatches, err := matchLevel(order, level)
			if err != nil {
				return nil, err
			}
			matches = append(matches, levelMatches...)
			if level.IsEmpty() {
				b.askLevels = b.askLevels[1:]
			}
			if order.Status() == OrderStatusFilled {
				break
			}

		}
	} else {
		if len(b.bidLevels) == 0 {
			return nil, fmt.Errorf("no buy side orders to match market order")
		}
		for len(b.bidLevels) > 0 {
			level := b.bidLevels[0]
			levelMatches, err := matchLevel(order, level)
			if err != nil {
				return nil, err
			}
			matches = append(matches, levelMatches...)
			if level.IsEmpty() {
				b.bidLevels = b.bidLevels[1:]
			}
			if order.Status() == OrderStatusFilled {
				break
			}

		}
	}
	return matches, nil
}

func (b *OrderBook) matchLimitOrder(order *Order) ([]*Order, error) {
	limitPrice := order.price
	matches := make([]*Order, 0)
	if order.side == BUY {
		for len(b.askLevels) > 0 {
			level := b.askLevels[0]
			levelPx := level.px
			if levelPx.GreaterThan(limitPrice) {
				break
			}
			levelMatches, err := matchLevel(order, level)
			if err != nil {
				return matches, err
			}
			matches = append(matches, levelMatches...)
			if level.IsEmpty() {
				b.askLevels = b.askLevels[1:]
			}
			if order.Status() == OrderStatusFilled {
				break
			}

		}
	} else {
		for len(b.bidLevels) > 0 {
			level := b.bidLevels[0]
			levelPx := level.px
			if levelPx.LessThan(limitPrice) {
				break
			}
			levelMatches, err := matchLevel(order, level)
			if err != nil {
				return matches, err
			}
			matches = append(matches, levelMatches...)
			if level.IsEmpty() {
				b.bidLevels = b.bidLevels[1:]
			}
			if order.Status() == OrderStatusFilled {
				break
			}

		}
	}
	if order.Status() != OrderStatusFilled {
		err := b.add(order)
		if err != nil {
			return matches, err
		}
	}
	return matches, nil
}

func (b *OrderBook) add(order *Order) error {
	px := order.price
	var levels []*bookLevel
	if order.side == BUY {
		levels = b.bidLevels
		for i := 0; i < len(levels); i++ {
			level := levels[i]
			if level.px.Equal(px) {
				err := level.Add(order)
				if err != nil {
					return err
				}
			} else if level.px.LessThan(px) {
				newLevel := newBookLevel([]*Order{}, px)
				err := newLevel.Add(order)
				if err != nil {
					return err
				}
				previousLevels := make([]*bookLevel, 0)
				if i > 0 {
					previousLevels = levels[:i-1]
				}
				remainingLevels := levels[i:]
				levels = append(previousLevels, newLevel)
				levels = append(levels, remainingLevels...)
			} else if i == len(levels)-1 {

				newLevel := newBookLevel([]*Order{}, px)
				levels = append(levels, newLevel)
			}
		}
		if len(levels) == 0 {

			newLevel := newBookLevel([]*Order{}, px)
			err := newLevel.Add(order)
			if err != nil {
				return err
			}
			levels = append(levels, newLevel)
		}
		b.bidLevels = levels
	} else {
		levels = b.askLevels
		for i := 0; i < len(levels); i++ {
			level := levels[i]
			if level.px.Equal(px) {
				err := level.Add(order)
				if err != nil {
					return err
				}
				break
			} else if level.px.GreaterThan(px) {
				newLevel := newBookLevel([]*Order{}, px)
				err := newLevel.Add(order)
				if err != nil {
					return err
				}
				previousLevels := make([]*bookLevel, 0)
				if i > 0 {
					previousLevels = levels[:i-1]
				}
				remainingLevels := levels[i:]
				levels = append(previousLevels, newLevel)
				levels = append(levels, remainingLevels...)
				break
			} else if i == len(levels)-1 {

				newLevel := newBookLevel([]*Order{}, px)
				levels = append(levels, newLevel)
			}
		}
		if len(levels) == 0 {

			newLevel := newBookLevel([]*Order{}, px)
			err := newLevel.Add(order)
			if err != nil {
				return err
			}
			levels = append(levels, newLevel)
		}
		b.askLevels = levels
	}

	return nil

}

func matchLevel(order *Order, level *bookLevel) ([]*Order, error) {
	matches := make([]*Order, 0)
	for len(level.orders) > 0 {
		bookOrd := level.orders[0]
		currentLeavesQty := order.LeavesQty()
		currBookOrdLeaves := bookOrd.LeavesQty()
		if currBookOrdLeaves.GreaterThanOrEqual(currentLeavesQty) {
			err := order.Execute(bookOrd.Price(), currentLeavesQty)
			if err != nil {
				return matches, err
			}
			err = bookOrd.Execute(bookOrd.Price(), currentLeavesQty)
			if err != nil {
				return matches, err
			}
		} else {
			err := bookOrd.Execute(bookOrd.Price(), currBookOrdLeaves)

			if err != nil {
				return matches, err
			}
			err = order.Execute(bookOrd.Price(), currBookOrdLeaves)
			if err != nil {
				return matches, err
			}
		}
		if bookOrd.Status() == OrderStatusFilled {
			_, err := level.Pop(bookOrd.clOrdID)
			if err != nil {
				return matches, err
			}
		}
		matches = append(matches, bookOrd)
		if order.Status() == OrderStatusFilled {
			break
		}

	}
	return matches, nil
}

func (b *OrderBook) Display() string {
	repr := "bid:\n"
	for i := 0; i < len(b.bidLevels); i++ {
		level := b.bidLevels[i]
		qty := decimal.Zero
		for _, order := range level.orders {
			qty = qty.Add(order.quantity)
		}
		repr += fmt.Sprintf("%.2f: %.2f\n", level.px.InexactFloat64(), qty.InexactFloat64())
	}
	repr += "\n=====\nask:\n"
	for i := 0; i < len(b.askLevels); i++ {
		level := b.bidLevels[i]
		qty := decimal.Zero
		for _, order := range level.orders {
			qty = qty.Add(order.quantity)
		}
		repr += fmt.Sprintf("%.2f: %.2f\n", level.px.InexactFloat64(), qty.InexactFloat64())
	}
	return repr
}
