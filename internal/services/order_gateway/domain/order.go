package domain

import (
	"errors"
	"fmt"
	"github.com/quickfixgo/enum"
	"github.com/shopspring/decimal"
	"time"
)

type OrderSide int

const (
	BUY  OrderSide = 1
	SELL OrderSide = 2
)

type OrderStatus int

const (
	OrderStatusOpen     OrderStatus = 1
	OrderStatusFilled               = 2
	OrderStatusRejected             = 3
	OrderStatusCanceled             = 4
)

type OrderExecution struct {
	quantity decimal.Decimal
	price    decimal.Decimal
	isFill   bool
}

func (o OrderExecution) Quantity() decimal.Decimal {
	return o.quantity
}

func (o OrderExecution) Price() decimal.Decimal {
	return o.price
}

func (o OrderExecution) IsFill() bool {
	return o.isFill
}

type Order struct {
	clOrdID          string
	symbol           string
	senderCompID     string
	targetCompID     string
	side             OrderSide
	ordType          enum.OrdType
	price            decimal.Decimal
	quantity         decimal.Decimal
	executedQuantity decimal.Decimal
	leavesQty        decimal.Decimal
	createdAt        time.Time
	lastExecQuantity decimal.Decimal
	lastExecPx       decimal.Decimal
	executedNotional decimal.Decimal
	status           OrderStatus
	executions       []*OrderExecution
	orderId          string
}

func (o *Order) Executions() []*OrderExecution {
	return o.executions
}

func (o *Order) ClOrdID() string {
	return o.clOrdID
}

func (o *Order) Symbol() string {
	return o.symbol
}

func (o *Order) SenderCompID() string {
	return o.senderCompID
}

func (o *Order) TargetCompID() string {
	return o.targetCompID
}

func (o *Order) Side() OrderSide {
	return o.side
}

func (o *Order) OrdType() enum.OrdType {
	return o.ordType
}

func (o *Order) Price() decimal.Decimal {
	return o.price
}

func (o *Order) Quantity() decimal.Decimal {
	return o.quantity
}

func (o *Order) ExecutedQuantity() decimal.Decimal {
	return o.executedQuantity
}

func (o *Order) CreatedAt() time.Time {
	return o.createdAt
}

func (o *Order) LastExecQuantity() decimal.Decimal {
	return o.lastExecQuantity
}

func (o *Order) LastExecPx() decimal.Decimal {
	return o.lastExecPx
}

func (o *Order) ExecutedNotional() decimal.Decimal {
	return o.executedNotional
}

func (o *Order) Status() OrderStatus {
	return o.status
}
func (o *Order) String() string {
	return fmt.Sprintf("clOrdID: %s\nsymbol: %s\nsenderCompID: %s\ntargetCompID: %s\nside: %d\nordType: %s\nprice: %s\nquantity: %s\nexecutedQuantity: %s\nleavesQty: %s\nlastExecQuantity: %s\nlastExecPx: %s\nexecutedNotional: %s\nstatus: %d",
		o.clOrdID, o.symbol, o.senderCompID, o.targetCompID, o.side, o.ordType, o.price, o.quantity, o.executedQuantity, o.leavesQty, o.lastExecQuantity, o.lastExecPx, o.executedNotional, o.status)
}

func NewOrder(clOrdID string,
	symbol string,
	senderCompID string,
	targetCompID string,
	side OrderSide,
	ordType enum.OrdType,
	price decimal.Decimal,
	quantity decimal.Decimal,
	orderId string,
) *Order {
	return &Order{
		clOrdID:          clOrdID,
		symbol:           symbol,
		senderCompID:     senderCompID,
		targetCompID:     targetCompID,
		side:             side,
		ordType:          ordType,
		price:            price,
		quantity:         quantity,
		executedQuantity: decimal.Zero,
		leavesQty:        quantity,
		executedNotional: decimal.Zero,
		createdAt:        time.Now().In(time.UTC),
		lastExecQuantity: decimal.Decimal{},
		lastExecPx:       decimal.Decimal{},
		status:           OrderStatusOpen,
		executions:       make([]*OrderExecution, 0),
		orderId:          orderId,
	}
}

func (o *Order) LeavesQty() decimal.Decimal {
	return o.leavesQty
}
func (o *Order) IsOpen() bool {
	return o.status == OrderStatusOpen
}

func (o *Order) Execute(price, quantity decimal.Decimal) error {
	//TODO make thread safe
	if quantity.GreaterThan(o.leavesQty) {
		return errors.New("quantity is greater than or equal to leavesQty")
	}
	o.executedQuantity = o.executedQuantity.Add(quantity)
	o.leavesQty = o.leavesQty.Sub(quantity)
	o.lastExecQuantity = quantity
	o.lastExecPx = price
	notional := price.Mul(quantity)
	o.executedNotional = o.executedNotional.Add(notional)
	if o.leavesQty.Equal(decimal.Zero) {
		o.status = OrderStatusFilled
	}
	o.executions = append(o.executions, &OrderExecution{
		quantity: quantity,
		price:    price,
		isFill:   o.executedQuantity.Equal(o.quantity),
	})
	return nil
}

func (o *Order) Cancel() {
	o.status = OrderStatusCanceled
}

func (o *Order) OrderID() string {
	return o.orderId
}
