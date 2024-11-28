package order_gateway

import (
	"context"
	"fmt"
	"github.com/quickfixgo/enum"
	"github.com/quickfixgo/field"
	"github.com/quickfixgo/fix44/executionreport"
	"github.com/quickfixgo/fix44/marketdatarequest"
	"github.com/quickfixgo/fix44/newordersingle"
	"github.com/quickfixgo/fix44/ordercancelrequest"
	"github.com/quickfixgo/quickfix"
	"log"
	"stock_exchange/internal/services/order_gateway/domain"
	"strconv"
	"time"
)

type Application struct {
	*quickfix.MessageRouter
	execID            int
	orderID           int
	orderBookBySymbol map[string]*domain.OrderBook
}

func NewApplication() *Application {
	app := &Application{
		MessageRouter:     quickfix.NewMessageRouter(),
		execID:            0,
		orderBookBySymbol: make(map[string]*domain.OrderBook),
	}
	app.AddRoute(newordersingle.Route(app.onNewOrderSingle))
	app.AddRoute(ordercancelrequest.Route(app.onOrderCancelRequest))
	app.AddRoute(marketdatarequest.Route(app.onMarketDataRequest))

	return app
}

// OnCreate implemented as part of Application interface
func (a Application) OnCreate(sessionID quickfix.SessionID) {}

// OnLogon implemented as part of Application interface
func (a Application) OnLogon(sessionID quickfix.SessionID) {}

// OnLogout implemented as part of Application interface
func (a Application) OnLogout(sessionID quickfix.SessionID) {}

// ToAdmin implemented as part of Application interface
func (a Application) ToAdmin(msg *quickfix.Message, sessionID quickfix.SessionID) {}

// ToApp implemented as part of Application interface
func (a Application) ToApp(msg *quickfix.Message, sessionID quickfix.SessionID) error {
	return nil
}

// FromAdmin implemented as part of Application interface
func (a Application) FromAdmin(msg *quickfix.Message, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	return nil
}

// FromApp implemented as part of Application interface, uses Router on incoming application messages
func (a *Application) FromApp(msg *quickfix.Message, sessionID quickfix.SessionID) (reject quickfix.MessageRejectError) {
	return a.Route(msg, sessionID)
}

func (a *Application) newOrderSingleToDomain(msg newordersingle.NewOrderSingle) (*domain.Order, quickfix.MessageRejectError) {
	a.orderID += 1
	orderID := a.orderID
	clOrdID, err := msg.GetClOrdID()
	if err != nil {
		return nil, err
	}

	symbol, err := msg.GetSymbol()
	if err != nil {
		return nil, err
	}

	senderCompID, err := msg.Header.GetSenderCompID()
	if err != nil {
		return nil, err
	}

	targetCompID, err := msg.Header.GetTargetCompID()
	if err != nil {
		return nil, err
	}

	side, err := msg.GetSide()
	if err != nil {
		return nil, err
	}

	ordType, err := msg.GetOrdType()
	if err != nil {
		return nil, err
	}

	price, err := msg.GetPrice()
	if err != nil {
		return nil, err
	}

	orderQty, err := msg.GetOrderQty()
	if err != nil {
		return nil, err
	}
	var domainSide domain.OrderSide
	switch side {
	case enum.Side_BUY:
		domainSide = domain.BUY
	case enum.Side_SELL:
		domainSide = domain.SELL
	}
	order := domain.NewOrder(clOrdID, symbol, senderCompID, targetCompID, domainSide, ordType, price, orderQty, strconv.Itoa(orderID))
	return order, nil
}

func (a *Application) onNewOrderSingle(msg newordersingle.NewOrderSingle, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	order, err := a.newOrderSingleToDomain(msg)
	if err != nil {
		return err
	}
	symbol := order.Symbol()
	book, ok := a.orderBookBySymbol[symbol]
	if !ok {
		book = domain.NewOrderBook(symbol)
		a.orderBookBySymbol[symbol] = book
	}
	matches, err2 := book.MatchOrAdd(context.TODO(), order)
	if err2 != nil {
		panic(err2)
	}
	log.Printf("%v", matches)
	log.Printf("%s", book.Display())
	events := make([]*ExecReportRequiredEvent, 0)
	for i := 0; i < len(matches); i++ {
		matched := matches[i]
		event := &ExecReportRequiredEvent{
			order:     matched,
			execution: matched.Executions()[len(matched.Executions())-1],
		}
		events = append(events, event)
		events = append(events, &ExecReportRequiredEvent{
			order:     order,
			execution: order.Executions()[i],
		})

	}
	go func() {
		_ = a.sendExecutionReports(events)

	}()
	return nil
}

func (a *Application) sendExecutionReports(events []*ExecReportRequiredEvent) error {
	for _, event := range events {
		log.Printf("%v", event)
		ordIDField := field.NewOrderID(event.order.OrderID())
		a.execID += 1
		execIDField := field.NewExecID(strconv.Itoa(a.execID))
		execType := enum.ExecType_PARTIAL_FILL
		if event.execution.IsFill() {
			execType = enum.ExecType_FILL
		}
		execTypeField := field.NewExecType(execType)
		var ordStatus enum.OrdStatus
		switch event.order.Status() {
		case domain.OrderStatusOpen:
			ordStatus = enum.OrdStatus_PARTIALLY_FILLED
		case domain.OrderStatusFilled:
			ordStatus = enum.OrdStatus_FILLED
		case domain.OrderStatusCanceled:
			break
		case domain.OrderStatusRejected:
			break
		}
		OrdStatusField := field.NewOrdStatus(ordStatus)
		var side enum.Side
		switch event.order.Side() {
		case domain.BUY:
			side = enum.Side_BUY
		case domain.SELL:
			side = enum.Side_SELL
		}
		sideField := field.NewSide(side)
		leavesQtyField := field.NewLeavesQty(event.order.LeavesQty(), 2)
		cumQtyField := field.NewCumQty(event.order.ExecutedQuantity(), 2)
		avgPxField := field.NewAvgPx(event.order.ExecutedNotional().Div(event.order.ExecutedQuantity()), 2)
		er := executionreport.New(ordIDField, execIDField, execTypeField, OrdStatusField, sideField, leavesQtyField, cumQtyField, avgPxField)
		er.SetLastQty(event.execution.Quantity(), 2)
		er.SetLastPx(event.execution.Price(), 2)
		msg := er.ToMessage()
		var err error
		for i := 0; i < 25; i++ {
			err = quickfix.SendToTarget(msg, quickfix.SessionID{
				BeginString:  quickfix.BeginStringFIX44,
				TargetCompID: event.order.SenderCompID(),
				SenderCompID: event.order.TargetCompID(),
			})
			if err == nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *Application) onOrderCancelRequest(msg ordercancelrequest.OrderCancelRequest, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	//origClOrdID, err := msg.GetOrigClOrdID()
	//if err != nil {
	//	return err
	//}
	//
	//symbol, err := msg.GetSymbol()
	//if err != nil {
	//	return err
	//}
	//
	//side, err := msg.GetSide()
	//if err != nil {
	//	return err
	//}
	//
	//order := a.Cancel(origClOrdID, symbol, side)
	//if order != nil {
	//	a.cancelOrder(*order)
	//}

	return nil
}

func (a *Application) onMarketDataRequest(msg marketdatarequest.MarketDataRequest, sessionID quickfix.SessionID) (err quickfix.MessageRejectError) {
	fmt.Printf("%+v\n", msg)
	return
}

//	func (a *Application) acceptOrder(order internal.Order) {
//		a.updateOrder(order, enum.OrdStatus_NEW)
//	}
//
//	func (a *Application) fillOrder(order internal.Order) {
//		status := enum.OrdStatus_FILLED
//		if !order.IsClosed() {
//			status = enum.OrdStatus_PARTIALLY_FILLED
//		}
//		a.updateOrder(order, status)
//	}
//
//	func (a *Application) cancelOrder(order internal.Order) {
//		a.updateOrder(order, enum.OrdStatus_CANCELED)
//	}
//
//	func (a *Application) genExecID() string {
//		a.execID++
//		return strconv.Itoa(a.execID)
//	}
//
//	func (a *Application) updateOrder(order internal.Order, status enum.OrdStatus) {
//		execReport := executionreport.New(
//			field.NewOrderID(order.ClOrdID),
//			field.NewExecID(a.genExecID()),
//			field.NewExecTransType(enum.ExecTransType_NEW),
//			field.NewExecType(enum.ExecType(status)),
//			field.NewOrdStatus(status),
//			field.NewSymbol(order.Symbol),
//			field.NewSide(order.Side),
//			field.NewLeavesQty(order.OpenQuantity(), 2),
//			field.NewCumQty(order.ExecutedQuantity, 2),
//			field.NewAvgPx(order.AvgPx, 2),
//		)
//		execReport.SetOrderQty(order.Quantity, 2)
//		execReport.SetClOrdID(order.ClOrdID)
//
//		switch status {
//		case enum.OrdStatus_FILLED, enum.OrdStatus_PARTIALLY_FILLED:
//			execReport.SetLastShares(order.LastExecutedQuantity, 2)
//			execReport.SetLastPx(order.LastExecutedPrice, 2)
//		}
//
//		execReport.Header.SetTargetCompID(order.SenderCompID)
//		execReport.Header.SetSenderCompID(order.TargetCompID)
//
//		sendErr := quickfix.Send(execReport)
//		if sendErr != nil {
//			fmt.Println(sendErr)
//		}
//
// }
type ExecReportRequiredEvent struct {
	order     *domain.Order
	execution *domain.OrderExecution
}
