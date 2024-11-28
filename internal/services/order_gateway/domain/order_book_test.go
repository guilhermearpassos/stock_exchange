package domain

import (
	"context"
	"github.com/quickfixgo/enum"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestOrderBook_MatchOrAddMarketOrder(t *testing.T) {
	sellPx, err := decimal.NewFromString("46.72")
	require.NoError(t, err)
	buyPx, err := decimal.NewFromString("46.52")
	require.NoError(t, err)
	tests := []struct {
		name      string
		setup     func(t *testing.T) *OrderBook
		order     *Order
		want      []*Order
		wantErr   bool
		checkBook func(t *testing.T, book *OrderBook)
	}{
		{
			name: "Market Order empty book",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol:    "VALE3",
					askLevels: []*bookLevel{},
					bidLevels: []*bookLevel{},
				}
			},
			order: NewOrder("1", "VALE3", "a", "b",
				BUY, enum.OrdType_MARKET, decimal.Decimal{}, decimal.NewFromInt(200), ""),
			want:    []*Order{},
			wantErr: true,
			checkBook: func(t *testing.T, book *OrderBook) {
				return
			},
		},
		{
			name: "SELL Market Order empty bid book",
			setup: func(t *testing.T) *OrderBook {
				px, err := decimal.NewFromString("46.72")
				require.NoError(t, err)
				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						{
							orders: []*Order{NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_MARKET, px, decimal.NewFromInt(200), "")},
							px: px,
						},
					},
					bidLevels: []*bookLevel{},
				}
			},
			order: NewOrder("1", "VALE3", "a", "b",
				SELL, enum.OrdType_MARKET, decimal.Decimal{}, decimal.NewFromInt(200), ""),
			want:    []*Order{},
			wantErr: true,
			checkBook: func(t *testing.T, book *OrderBook) {
				return
			},
		},
		{
			name: "BUY Market Order empty ask book",
			setup: func(t *testing.T) *OrderBook {
				px, err := decimal.NewFromString("46.72")
				require.NoError(t, err)
				return &OrderBook{
					symbol:    "VALE3",
					askLevels: []*bookLevel{},
					bidLevels: []*bookLevel{
						{
							orders: []*Order{NewOrder("2", "VALE3", "a", "b",
								BUY, enum.OrdType_MARKET, px, decimal.NewFromInt(200), "")},
							px: px,
						}},
				}
			},
			order: NewOrder("1", "VALE3", "a", "b",
				BUY, enum.OrdType_MARKET, decimal.Decimal{}, decimal.NewFromInt(200), ""),
			want:    []*Order{},
			wantErr: true,
			checkBook: func(t *testing.T, book *OrderBook) {
				return
			},
		},
		{
			name: "SELL Market Order single Level book - equal sze/price",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol:    "VALE3",
					askLevels: []*bookLevel{},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, sellPx, decimal.NewFromInt(200), ""),
						}, sellPx)},
				}
			},
			order: NewOrder("1", "VALE3", "a", "b",
				SELL, enum.OrdType_MARKET, decimal.Decimal{}, decimal.NewFromInt(200), ""),
			want: []*Order{
				{
					clOrdID:          "2",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             BUY,
					ordType:          enum.OrdType_LIMIT,
					price:            sellPx,
					quantity:         decimal.NewFromInt(200),
					executedQuantity: decimal.NewFromInt(200),
					leavesQty:        decimal.Zero,
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(200),
					lastExecPx:       sellPx,
					executedNotional: decimal.NewFromInt(200).Mul(sellPx),
					status:           OrderStatusFilled,
					executions:       make([]*OrderExecution, 0),
				},
				//NewOrder("2", "VALE3", "a", "b",
				//	BUY, enum.OrdType_LIMIT, sellPx, decimal.NewFromInt(200), ""),
			},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {
				return
			},
		},
		{
			name: "BUY Market Order single Level book - equal sze/price",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx, decimal.NewFromInt(200), ""),
						}, sellPx),
					},
					bidLevels: []*bookLevel{},
				}
			},
			order: NewOrder("1", "VALE3", "a", "b",
				BUY, enum.OrdType_MARKET, decimal.Decimal{}, decimal.NewFromInt(200), ""),
			want: []*Order{
				{
					clOrdID:          "2",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             SELL,
					ordType:          enum.OrdType_LIMIT,
					price:            sellPx,
					quantity:         decimal.NewFromInt(200),
					executedQuantity: decimal.NewFromInt(200),
					leavesQty:        decimal.Zero,
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(200),
					lastExecPx:       sellPx,
					executedNotional: decimal.NewFromInt(200).Mul(sellPx),
					status:           OrderStatusFilled,
					executions:       make([]*OrderExecution, 0),
				},
				//NewOrder("2", "VALE3", "a", "b",
				//	BUY, enum.OrdType_LIMIT, sellPx, decimal.NewFromInt(200), ""),
			},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {
				return
			},
		},
		{
			name: "BUY Market Order full single Level book - equal sze/price",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx, decimal.NewFromInt(200), ""),
						}, sellPx),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx, decimal.NewFromInt(150), ""),
						}, buyPx),
					},
				}
			},
			order: NewOrder("1", "VALE3", "a", "b",
				BUY, enum.OrdType_MARKET, decimal.Decimal{}, decimal.NewFromInt(200), ""),
			want: []*Order{
				{
					clOrdID:          "2",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             SELL,
					ordType:          enum.OrdType_LIMIT,
					price:            sellPx,
					quantity:         decimal.NewFromInt(200),
					executedQuantity: decimal.NewFromInt(200),
					leavesQty:        decimal.Zero,
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(200),
					lastExecPx:       sellPx,
					executedNotional: decimal.NewFromInt(200).Mul(sellPx),
					status:           OrderStatusFilled,
					executions:       make([]*OrderExecution, 0),
				},
				//NewOrder("2", "VALE3", "a", "b",
				//	BUY, enum.OrdType_LIMIT, sellPx, decimal.NewFromInt(200), ""),
			},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {
				return
			},
		},
		{
			name: "SELL Market Order full single Level book - equal sze/price",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx, decimal.NewFromInt(200), ""),
						}, sellPx),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx, decimal.NewFromInt(150), ""),
						}, buyPx),
					},
				}
			},
			order: NewOrder("1", "VALE3", "a", "b",
				SELL, enum.OrdType_MARKET, decimal.Decimal{}, decimal.NewFromInt(150), ""),
			want: []*Order{
				{
					clOrdID:          "3",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             BUY,
					ordType:          enum.OrdType_LIMIT,
					price:            buyPx,
					quantity:         decimal.NewFromInt(150),
					executedQuantity: decimal.NewFromInt(150),
					leavesQty:        decimal.Zero,
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(150),
					lastExecPx:       buyPx,
					executedNotional: decimal.NewFromInt(150).Mul(buyPx),
					status:           OrderStatusFilled,
					executions:       make([]*OrderExecution, 0),
				},
				//NewOrder("2", "VALE3", "a", "b",
				//	BUY, enum.OrdType_LIMIT, sellPx, decimal.NewFromInt(200), ""),
			},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {
				return
			},
		},
		{
			name: "SELL Market Order full book",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx, decimal.NewFromInt(200), ""),
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx.Add(decimal.NewFromFloat(0.01)), decimal.NewFromInt(200), ""),
							NewOrder("4", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx.Add(decimal.NewFromFloat(0.02)), decimal.NewFromInt(150), ""),
							NewOrder("5", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx.Add(decimal.NewFromFloat(0.03)), decimal.NewFromInt(50), ""),
							NewOrder("6", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx.Add(decimal.NewFromFloat(0.04)), decimal.NewFromInt(200), ""),
							NewOrder("7", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx.Add(decimal.NewFromFloat(0.05)), decimal.NewFromInt(200), ""),
						}, sellPx),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("8", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx, decimal.NewFromInt(150), ""),
							NewOrder("9", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx.Sub(decimal.NewFromFloat(0.01)), decimal.NewFromInt(200), ""),
							NewOrder("10", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx.Sub(decimal.NewFromFloat(0.02)), decimal.NewFromInt(150), ""),
							NewOrder("11", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx.Sub(decimal.NewFromFloat(0.03)), decimal.NewFromInt(50), ""),
							NewOrder("12", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx.Sub(decimal.NewFromFloat(0.04)), decimal.NewFromInt(200), ""),
							NewOrder("13", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx.Sub(decimal.NewFromFloat(0.05)), decimal.NewFromInt(200), ""),
						}, buyPx),
					},
				}
			},
			order: NewOrder("1", "VALE3", "a", "b",
				SELL, enum.OrdType_MARKET, decimal.Decimal{}, decimal.NewFromInt(525), ""),
			want: []*Order{
				{
					clOrdID:          "8",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             BUY,
					ordType:          enum.OrdType_LIMIT,
					price:            buyPx,
					quantity:         decimal.NewFromInt(150),
					executedQuantity: decimal.NewFromInt(150),
					leavesQty:        decimal.Zero,
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(150),
					lastExecPx:       buyPx,
					executedNotional: decimal.NewFromInt(150).Mul(buyPx),
					status:           OrderStatusFilled,
					executions:       make([]*OrderExecution, 0),
				},
				{
					clOrdID:          "9",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             BUY,
					ordType:          enum.OrdType_LIMIT,
					price:            buyPx.Sub(decimal.NewFromFloat(0.01)),
					quantity:         decimal.NewFromInt(200),
					executedQuantity: decimal.NewFromInt(200),
					leavesQty:        decimal.Zero,
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(200),
					lastExecPx:       buyPx.Sub(decimal.NewFromFloat(0.01)),
					executedNotional: decimal.NewFromInt(200).Mul(buyPx.Sub(decimal.NewFromFloat(0.01))),
					status:           OrderStatusFilled,
					executions:       make([]*OrderExecution, 0),
				},
				{
					clOrdID:          "10",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             BUY,
					ordType:          enum.OrdType_LIMIT,
					price:            buyPx.Sub(decimal.NewFromFloat(0.02)),
					quantity:         decimal.NewFromInt(150),
					executedQuantity: decimal.NewFromInt(150),
					leavesQty:        decimal.Zero,
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(150),
					lastExecPx:       buyPx.Sub(decimal.NewFromFloat(0.02)),
					executedNotional: decimal.NewFromInt(150).Mul(buyPx.Sub(decimal.NewFromFloat(0.02))),
					status:           OrderStatusFilled,
					executions:       make([]*OrderExecution, 0),
				},
				{
					clOrdID:          "11",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             BUY,
					ordType:          enum.OrdType_LIMIT,
					price:            buyPx.Sub(decimal.NewFromFloat(0.03)),
					quantity:         decimal.NewFromInt(50),
					executedQuantity: decimal.NewFromInt(25),
					leavesQty:        decimal.NewFromInt(25),
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(25),
					lastExecPx:       buyPx.Sub(decimal.NewFromFloat(0.03)),
					executedNotional: decimal.NewFromInt(25).Mul(buyPx.Sub(decimal.NewFromFloat(0.03))),
					status:           OrderStatusOpen,
					executions:       make([]*OrderExecution, 0),
				},
				//NewOrder("2", "VALE3", "a", "b",
				//	BUY, enum.OrdType_LIMIT, sellPx, decimal.NewFromInt(200), ""),
			},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {
				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx, decimal.NewFromInt(200), ""),
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx.Add(decimal.NewFromFloat(0.01)), decimal.NewFromInt(200), ""),
							NewOrder("4", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx.Add(decimal.NewFromFloat(0.02)), decimal.NewFromInt(150), ""),
							NewOrder("5", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx.Add(decimal.NewFromFloat(0.03)), decimal.NewFromInt(50), ""),
							NewOrder("6", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx.Add(decimal.NewFromFloat(0.04)), decimal.NewFromInt(200), ""),
							NewOrder("7", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx.Add(decimal.NewFromFloat(0.05)), decimal.NewFromInt(200), ""),
						}, sellPx),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							{
								clOrdID:          "11",
								symbol:           "VALE3",
								senderCompID:     "a",
								targetCompID:     "b",
								side:             BUY,
								ordType:          enum.OrdType_LIMIT,
								price:            buyPx.Sub(decimal.NewFromFloat(0.03)),
								quantity:         decimal.NewFromInt(50),
								executedQuantity: decimal.NewFromInt(25),
								leavesQty:        decimal.NewFromInt(25),
								createdAt:        time.Now().In(time.UTC),
								lastExecQuantity: decimal.NewFromInt(25),
								lastExecPx:       buyPx.Sub(decimal.NewFromFloat(0.03)),
								executedNotional: decimal.NewFromInt(25).Mul(buyPx.Sub(decimal.NewFromFloat(0.03))),
								status:           OrderStatusOpen,
								executions:       make([]*OrderExecution, 0),
							},
							NewOrder("12", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx.Sub(decimal.NewFromFloat(0.04)), decimal.NewFromInt(200), ""),
							NewOrder("13", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx.Sub(decimal.NewFromFloat(0.05)), decimal.NewFromInt(200), ""),
						}, buyPx),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.setup(t)
			got, err := b.MatchOrAdd(context.Background(), tt.order)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if !compareOrderSlice(got, tt.want) {
				t.Errorf("MatchOrAdd() got = %v, want %v", got, tt.want)
				return
			}
			if tt.checkBook != nil {
				tt.checkBook(t, b)
			}
		})
	}
}

func assertBooksEqual(t *testing.T, book *OrderBook, wantBook OrderBook) {
	display := book.Display()
	wantDisplay := wantBook.Display()
	if display != wantDisplay {
		t.Errorf("books display differ: want %s got %s", wantDisplay, display)
	}
	if len(book.askLevels) != len(wantBook.askLevels) {
		t.Errorf("book levels differ - ask %d vs %d", len(book.askLevels), len(wantBook.askLevels))
	}
	if len(book.bidLevels) != len(wantBook.bidLevels) {
		t.Errorf("book levels differ - bid %d vs %d", len(book.bidLevels), len(wantBook.bidLevels))
	}
	for i := 0; i < len(book.askLevels); i++ {
		if !compareOrderSlice(book.askLevels[i].orders, wantBook.askLevels[i].orders) {
			t.Errorf("book levels differ - ask level %d, got %v want %v", i, book.askLevels[i].orders, wantBook.askLevels[i].orders)
		}
	}
	for i := 0; i < len(book.bidLevels); i++ {
		if !compareOrderSlice(book.bidLevels[i].orders, wantBook.bidLevels[i].orders) {
			t.Errorf("book levels differ - bid level %d, got %v want %v", i, book.bidLevels[i].orders, wantBook.bidLevels[i].orders)
		}
	}
}

func TestOrderBook_AddLimitOrder(t *testing.T) {
	sellPx1, _ := decimal.NewFromString("46.72")
	sellPx2, _ := decimal.NewFromString("46.73")
	sellPx3, _ := decimal.NewFromString("46.74")
	//sellPx4, _ := decimal.NewFromString("46.75")
	buyPx1, _ := decimal.NewFromString("46.52")
	buyPx2, _ := decimal.NewFromString("46.51")
	buyPx3, _ := decimal.NewFromString("46.50")
	//buyPx4, _ := decimal.NewFromString("46.49")
	buyPxClose, _ := decimal.NewFromString("46.71")
	tests := []struct {
		name      string
		setup     func(t *testing.T) *OrderBook
		order     *Order
		want      []*Order
		wantErr   bool
		checkBook func(t *testing.T, book *OrderBook)
	}{
		{
			name: "Limit Order empty book",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol:    "VALE3",
					askLevels: []*bookLevel{},
					bidLevels: []*bookLevel{},
				}
			},
			order: NewOrder("1", "VALE3", "a", "b",
				BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
			want:    []*Order{},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol:    "VALE3",
					askLevels: []*bookLevel{},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "SELL Limit Order empty book",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol:    "VALE3",
					askLevels: []*bookLevel{},
					bidLevels: []*bookLevel{},
				}
			},
			order: NewOrder("1", "VALE3", "a", "b",
				SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
			want:    []*Order{},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
					},
					bidLevels: []*bookLevel{},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},

		{
			name: "SELL Limit Order add new level to the bottom",
			setup: func(t *testing.T) *OrderBook {

				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
					},
					bidLevels: []*bookLevel{},
				}
			},
			order: NewOrder("2", "VALE3", "a", "b",
				SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
			want:    []*Order{},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
					},
					bidLevels: []*bookLevel{},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "SELL Limit Order add new level to the top",
			setup: func(t *testing.T) *OrderBook {

				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
					},
					bidLevels: []*bookLevel{},
				}
			},
			order: NewOrder("1", "VALE3", "a", "b",
				SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
			want:    []*Order{},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
					},
					bidLevels: []*bookLevel{},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "BUY Limit Order add new level to the bottom",
			setup: func(t *testing.T) *OrderBook {

				return &OrderBook{
					symbol:    "VALE3",
					askLevels: []*bookLevel{},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1)},
				}
			},
			order: NewOrder("2", "VALE3", "a", "b",
				BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
			want:    []*Order{},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol:    "VALE3",
					askLevels: []*bookLevel{},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "BUY Limit Order add new level to the top",
			setup: func(t *testing.T) *OrderBook {

				return &OrderBook{
					symbol:    "VALE3",
					askLevels: []*bookLevel{},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2)},
				}
			},
			order: NewOrder("1", "VALE3", "a", "b",
				BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
			want:    []*Order{},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol:    "VALE3",
					askLevels: []*bookLevel{},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "BUY Limit Order add new level to the full book",
			setup: func(t *testing.T) *OrderBook {

				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
			},
			order: NewOrder("7", "VALE3", "a", "b",
				BUY, enum.OrdType_LIMIT, buyPxClose, decimal.NewFromInt(200), ""),
			want:    []*Order{},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("7", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPxClose, decimal.NewFromInt(200), ""),
						}, buyPxClose),
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "BUY Limit Order add to existing level to the full book",
			setup: func(t *testing.T) *OrderBook {

				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
			},
			order: NewOrder("7", "VALE3", "a", "b",
				BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
			want:    []*Order{},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
							NewOrder("7", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "SELL Limit Order add to existing level to the full book",
			setup: func(t *testing.T) *OrderBook {

				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
			},
			order: NewOrder("7", "VALE3", "a", "b",
				SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(250), ""),
			want:    []*Order{},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
							NewOrder("7", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(250), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "SELL Limit Order add new middle level",
			setup: func(t *testing.T) *OrderBook {

				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
			},
			order: NewOrder("7", "VALE3", "a", "b",
				SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(250), ""),
			want:    []*Order{},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("7", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(250), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "BUY Limit Order add new middle level",
			setup: func(t *testing.T) *OrderBook {

				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
			},
			order: NewOrder("7", "VALE3", "a", "b",
				BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
			want:    []*Order{},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("7", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.setup(t)
			got, err := b.MatchOrAdd(context.Background(), tt.order)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if !compareOrderSlice(got, tt.want) {
				t.Errorf("MatchOrAdd() got = %v, want %v", got, tt.want)
				return
			}
			if tt.checkBook != nil {
				tt.checkBook(t, b)
			}
		})
	}
}
func TestOrderBook_MatchLimitOrder(t *testing.T) {
	sellPx1, _ := decimal.NewFromString("46.72")
	sellPx2, _ := decimal.NewFromString("46.73")
	sellPx3, _ := decimal.NewFromString("46.74")
	//sellPx4, _ := decimal.NewFromString("46.75")
	buyPx1, _ := decimal.NewFromString("46.52")
	buyPx2, _ := decimal.NewFromString("46.51")
	buyPx3, _ := decimal.NewFromString("46.50")
	//buyPx4, _ := decimal.NewFromString("46.49")
	buyPxClose, _ := decimal.NewFromString("46.71")
	tests := []struct {
		name      string
		setup     func(t *testing.T) *OrderBook
		order     *Order
		want      []*Order
		wantOrder *Order
		wantErr   bool
		checkBook func(t *testing.T, book *OrderBook)
	}{
		{
			name: "BUY Limit Order filled but partial first level match",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("7", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPxClose, decimal.NewFromInt(200), ""),
						}, buyPxClose),
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
			},
			order: NewOrder("11", "VALE3", "a", "b",
				BUY, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(100), ""),
			want: []*Order{

				{
					clOrdID:          "1",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             SELL,
					ordType:          enum.OrdType_LIMIT,
					price:            sellPx1,
					quantity:         decimal.NewFromInt(200),
					executedQuantity: decimal.NewFromInt(100),
					leavesQty:        decimal.NewFromInt(100),
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(100),
					lastExecPx:       sellPx1,
					executedNotional: decimal.NewFromInt(100).Mul(sellPx1),
					status:           OrderStatusOpen,
					executions:       make([]*OrderExecution, 0),
				},
			},
			wantOrder: &Order{
				clOrdID:          "11",
				symbol:           "VALE3",
				senderCompID:     "a",
				targetCompID:     "b",
				side:             BUY,
				ordType:          enum.OrdType_LIMIT,
				price:            sellPx1,
				quantity:         decimal.NewFromInt(100),
				executedQuantity: decimal.NewFromInt(100),
				leavesQty:        decimal.NewFromInt(0),
				createdAt:        time.Now().In(time.UTC),
				lastExecQuantity: decimal.NewFromInt(100),
				lastExecPx:       sellPx1,
				executedNotional: decimal.NewFromInt(100).Mul(sellPx1),
				status:           OrderStatusFilled,
				executions:       make([]*OrderExecution, 0),
			},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							{
								clOrdID:          "1",
								symbol:           "VALE3",
								senderCompID:     "a",
								targetCompID:     "b",
								side:             SELL,
								ordType:          enum.OrdType_LIMIT,
								price:            sellPx1,
								quantity:         decimal.NewFromInt(200),
								executedQuantity: decimal.NewFromInt(100),
								leavesQty:        decimal.NewFromInt(100),
								createdAt:        time.Now().In(time.UTC),
								lastExecQuantity: decimal.NewFromInt(100),
								lastExecPx:       sellPx1,
								executedNotional: decimal.NewFromInt(100).Mul(sellPx1),
								status:           OrderStatusOpen,
								executions:       make([]*OrderExecution, 0),
							},
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("7", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPxClose, decimal.NewFromInt(200), ""),
						}, buyPxClose),
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "SELL Limit Order filled but partial first level match",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("7", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPxClose, decimal.NewFromInt(200), ""),
						}, buyPxClose),
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
			},
			order: NewOrder("11", "VALE3", "a", "b",
				SELL, enum.OrdType_LIMIT, buyPxClose, decimal.NewFromInt(100), ""),
			want: []*Order{

				{
					clOrdID:          "7",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             BUY,
					ordType:          enum.OrdType_LIMIT,
					price:            buyPxClose,
					quantity:         decimal.NewFromInt(200),
					executedQuantity: decimal.NewFromInt(100),
					leavesQty:        decimal.NewFromInt(100),
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(100),
					lastExecPx:       buyPxClose,
					executedNotional: decimal.NewFromInt(100).Mul(buyPxClose),
					status:           OrderStatusOpen,
					executions:       make([]*OrderExecution, 0),
				},
			},
			wantOrder: &Order{
				clOrdID:          "11",
				symbol:           "VALE3",
				senderCompID:     "a",
				targetCompID:     "b",
				side:             SELL,
				ordType:          enum.OrdType_LIMIT,
				price:            buyPxClose,
				quantity:         decimal.NewFromInt(100),
				executedQuantity: decimal.NewFromInt(100),
				leavesQty:        decimal.NewFromInt(0),
				createdAt:        time.Now().In(time.UTC),
				lastExecQuantity: decimal.NewFromInt(100),
				lastExecPx:       buyPxClose,
				executedNotional: decimal.NewFromInt(100).Mul(buyPxClose),
				status:           OrderStatusFilled,
				executions:       make([]*OrderExecution, 0),
			},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							{
								clOrdID:          "7",
								symbol:           "VALE3",
								senderCompID:     "a",
								targetCompID:     "b",
								side:             BUY,
								ordType:          enum.OrdType_LIMIT,
								price:            buyPxClose,
								quantity:         decimal.NewFromInt(200),
								executedQuantity: decimal.NewFromInt(100),
								leavesQty:        decimal.NewFromInt(100),
								createdAt:        time.Now().In(time.UTC),
								lastExecQuantity: decimal.NewFromInt(100),
								lastExecPx:       buyPxClose,
								executedNotional: decimal.NewFromInt(100).Mul(buyPxClose),
								status:           OrderStatusOpen,
								executions:       make([]*OrderExecution, 0),
							},
						}, buyPxClose),
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "BUY Limit Order filled but partial first level match - agressive limit",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("7", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPxClose, decimal.NewFromInt(200), ""),
						}, buyPxClose),
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
			},
			order: NewOrder("11", "VALE3", "a", "b",
				BUY, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(100), ""),
			want: []*Order{

				{
					clOrdID:          "1",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             SELL,
					ordType:          enum.OrdType_LIMIT,
					price:            sellPx1,
					quantity:         decimal.NewFromInt(200),
					executedQuantity: decimal.NewFromInt(100),
					leavesQty:        decimal.NewFromInt(100),
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(100),
					lastExecPx:       sellPx1,
					executedNotional: decimal.NewFromInt(100).Mul(sellPx1),
					status:           OrderStatusOpen,
					executions:       make([]*OrderExecution, 0),
				},
			},
			wantOrder: &Order{
				clOrdID:          "11",
				symbol:           "VALE3",
				senderCompID:     "a",
				targetCompID:     "b",
				side:             BUY,
				ordType:          enum.OrdType_LIMIT,
				price:            sellPx3,
				quantity:         decimal.NewFromInt(100),
				executedQuantity: decimal.NewFromInt(100),
				leavesQty:        decimal.NewFromInt(0),
				createdAt:        time.Now().In(time.UTC),
				lastExecQuantity: decimal.NewFromInt(100),
				lastExecPx:       sellPx1,
				executedNotional: decimal.NewFromInt(100).Mul(sellPx1),
				status:           OrderStatusFilled,
				executions:       make([]*OrderExecution, 0),
			},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							{
								clOrdID:          "1",
								symbol:           "VALE3",
								senderCompID:     "a",
								targetCompID:     "b",
								side:             SELL,
								ordType:          enum.OrdType_LIMIT,
								price:            sellPx1,
								quantity:         decimal.NewFromInt(200),
								executedQuantity: decimal.NewFromInt(100),
								leavesQty:        decimal.NewFromInt(100),
								createdAt:        time.Now().In(time.UTC),
								lastExecQuantity: decimal.NewFromInt(100),
								lastExecPx:       sellPx1,
								executedNotional: decimal.NewFromInt(100).Mul(sellPx1),
								status:           OrderStatusOpen,
								executions:       make([]*OrderExecution, 0),
							},
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("7", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPxClose, decimal.NewFromInt(200), ""),
						}, buyPxClose),
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "SELL Limit Order filled but partial first level match - agressive limit",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("7", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPxClose, decimal.NewFromInt(200), ""),
						}, buyPxClose),
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
			},
			order: NewOrder("11", "VALE3", "a", "b",
				SELL, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(100), ""),
			want: []*Order{

				{
					clOrdID:          "7",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             BUY,
					ordType:          enum.OrdType_LIMIT,
					price:            buyPxClose,
					quantity:         decimal.NewFromInt(200),
					executedQuantity: decimal.NewFromInt(100),
					leavesQty:        decimal.NewFromInt(100),
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(100),
					lastExecPx:       buyPxClose,
					executedNotional: decimal.NewFromInt(100).Mul(buyPxClose),
					status:           OrderStatusOpen,
					executions:       make([]*OrderExecution, 0),
				},
			},
			wantOrder: &Order{
				clOrdID:          "11",
				symbol:           "VALE3",
				senderCompID:     "a",
				targetCompID:     "b",
				side:             SELL,
				ordType:          enum.OrdType_LIMIT,
				price:            buyPx3,
				quantity:         decimal.NewFromInt(100),
				executedQuantity: decimal.NewFromInt(100),
				leavesQty:        decimal.NewFromInt(0),
				createdAt:        time.Now().In(time.UTC),
				lastExecQuantity: decimal.NewFromInt(100),
				lastExecPx:       buyPxClose,
				executedNotional: decimal.NewFromInt(100).Mul(buyPxClose),
				status:           OrderStatusFilled,
				executions:       make([]*OrderExecution, 0),
			},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							{
								clOrdID:          "7",
								symbol:           "VALE3",
								senderCompID:     "a",
								targetCompID:     "b",
								side:             BUY,
								ordType:          enum.OrdType_LIMIT,
								price:            buyPxClose,
								quantity:         decimal.NewFromInt(200),
								executedQuantity: decimal.NewFromInt(100),
								leavesQty:        decimal.NewFromInt(100),
								createdAt:        time.Now().In(time.UTC),
								lastExecQuantity: decimal.NewFromInt(100),
								lastExecPx:       buyPxClose,
								executedNotional: decimal.NewFromInt(100).Mul(buyPxClose),
								status:           OrderStatusOpen,
								executions:       make([]*OrderExecution, 0),
							},
						}, buyPxClose),
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "BUY Limit Order partial filling whole first level",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("7", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPxClose, decimal.NewFromInt(200), ""),
						}, buyPxClose),
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
			},
			order: NewOrder("11", "VALE3", "a", "b",
				BUY, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(1000), ""),
			want: []*Order{

				{
					clOrdID:          "1",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             SELL,
					ordType:          enum.OrdType_LIMIT,
					price:            sellPx1,
					quantity:         decimal.NewFromInt(200),
					executedQuantity: decimal.NewFromInt(200),
					leavesQty:        decimal.NewFromInt(0),
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(200),
					lastExecPx:       sellPx1,
					executedNotional: decimal.NewFromInt(200).Mul(sellPx1),
					status:           OrderStatusFilled,
					executions:       make([]*OrderExecution, 0),
				},
			},
			wantOrder: &Order{
				clOrdID:          "11",
				symbol:           "VALE3",
				senderCompID:     "a",
				targetCompID:     "b",
				side:             BUY,
				ordType:          enum.OrdType_LIMIT,
				price:            sellPx1,
				quantity:         decimal.NewFromInt(1000),
				executedQuantity: decimal.NewFromInt(200),
				leavesQty:        decimal.NewFromInt(800),
				createdAt:        time.Now().In(time.UTC),
				lastExecQuantity: decimal.NewFromInt(200),
				lastExecPx:       sellPx1,
				executedNotional: decimal.NewFromInt(200).Mul(sellPx1),
				status:           OrderStatusOpen,
				executions:       make([]*OrderExecution, 0),
			},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{{
							clOrdID:          "11",
							symbol:           "VALE3",
							senderCompID:     "a",
							targetCompID:     "b",
							side:             BUY,
							ordType:          enum.OrdType_LIMIT,
							price:            sellPx1,
							quantity:         decimal.NewFromInt(1000),
							executedQuantity: decimal.NewFromInt(200),
							leavesQty:        decimal.NewFromInt(800),
							createdAt:        time.Now().In(time.UTC),
							lastExecQuantity: decimal.NewFromInt(200),
							lastExecPx:       sellPx1,
							executedNotional: decimal.NewFromInt(200).Mul(sellPx1),
							status:           OrderStatusOpen,
							executions:       make([]*OrderExecution, 0),
						}}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("7", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPxClose, decimal.NewFromInt(200), ""),
						}, buyPxClose),
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
		{
			name: "SELL Limit Order partial filling whole first level",
			setup: func(t *testing.T) *OrderBook {
				return &OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("7", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPxClose, decimal.NewFromInt(200), ""),
						}, buyPxClose),
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
			},
			order: NewOrder("11", "VALE3", "a", "b",
				SELL, enum.OrdType_LIMIT, buyPxClose, decimal.NewFromInt(1000), ""),
			want: []*Order{

				{
					clOrdID:          "7",
					symbol:           "VALE3",
					senderCompID:     "a",
					targetCompID:     "b",
					side:             BUY,
					ordType:          enum.OrdType_LIMIT,
					price:            buyPxClose,
					quantity:         decimal.NewFromInt(200),
					executedQuantity: decimal.NewFromInt(200),
					leavesQty:        decimal.NewFromInt(0),
					createdAt:        time.Now().In(time.UTC),
					lastExecQuantity: decimal.NewFromInt(200),
					lastExecPx:       buyPxClose,
					executedNotional: decimal.NewFromInt(200).Mul(buyPxClose),
					status:           OrderStatusFilled,
					executions:       make([]*OrderExecution, 0),
				},
			},
			wantOrder: &Order{
				clOrdID:          "11",
				symbol:           "VALE3",
				senderCompID:     "a",
				targetCompID:     "b",
				side:             SELL,
				ordType:          enum.OrdType_LIMIT,
				price:            buyPxClose,
				quantity:         decimal.NewFromInt(1000),
				executedQuantity: decimal.NewFromInt(200),
				leavesQty:        decimal.NewFromInt(800),
				createdAt:        time.Now().In(time.UTC),
				lastExecQuantity: decimal.NewFromInt(200),
				lastExecPx:       buyPxClose,
				executedNotional: decimal.NewFromInt(200).Mul(buyPxClose),
				status:           OrderStatusOpen,
				executions:       make([]*OrderExecution, 0),
			},
			wantErr: false,
			checkBook: func(t *testing.T, book *OrderBook) {

				wantBook := OrderBook{
					symbol: "VALE3",
					askLevels: []*bookLevel{
						newBookLevel([]*Order{
							{
								clOrdID:          "11",
								symbol:           "VALE3",
								senderCompID:     "a",
								targetCompID:     "b",
								side:             SELL,
								ordType:          enum.OrdType_LIMIT,
								price:            buyPxClose,
								quantity:         decimal.NewFromInt(1000),
								executedQuantity: decimal.NewFromInt(200),
								leavesQty:        decimal.NewFromInt(800),
								createdAt:        time.Now().In(time.UTC),
								lastExecQuantity: decimal.NewFromInt(200),
								lastExecPx:       buyPxClose,
								executedNotional: decimal.NewFromInt(200).Mul(buyPxClose),
								status:           OrderStatusOpen,
								executions:       make([]*OrderExecution, 0),
							},
						}, buyPxClose),
						newBookLevel([]*Order{
							NewOrder("1", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx1, decimal.NewFromInt(200), ""),
						}, sellPx1),
						newBookLevel([]*Order{
							NewOrder("2", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx2, decimal.NewFromInt(200), ""),
						}, sellPx2),
						newBookLevel([]*Order{
							NewOrder("3", "VALE3", "a", "b",
								SELL, enum.OrdType_LIMIT, sellPx3, decimal.NewFromInt(200), ""),
						}, sellPx3),
					},
					bidLevels: []*bookLevel{
						newBookLevel([]*Order{
							NewOrder("4", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx1, decimal.NewFromInt(200), ""),
						}, buyPx1),
						newBookLevel([]*Order{
							NewOrder("5", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx2, decimal.NewFromInt(200), ""),
						}, buyPx2),
						newBookLevel([]*Order{
							NewOrder("6", "VALE3", "a", "b",
								BUY, enum.OrdType_LIMIT, buyPx3, decimal.NewFromInt(200), ""),
						}, buyPx3),
					},
				}
				assertBooksEqual(t, book, wantBook)
				return
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.setup(t)
			got, err := b.MatchOrAdd(context.Background(), tt.order)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if !compareOrder(*tt.order, *tt.wantOrder) {
				t.Errorf("final status of order diffs: got = %v, want = %v", tt.order, tt.wantOrder)
			}
			if !compareOrderSlice(got, tt.want) {
				t.Errorf("MatchOrAdd() got = %v, want %v", got, tt.want)
				return
			}
			if tt.checkBook != nil {
				tt.checkBook(t, b)
			}
		})
	}
}

func compareOrderSlice(a, b []*Order) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if !compareOrder(*a[i], *b[i]) {
			return false
		}
	}
	return true
}

func compareOrder(a, b Order) bool {
	if a.clOrdID != b.clOrdID {
		return false
	}
	if a.symbol != b.symbol {
		return false
	}
	if a.senderCompID != b.senderCompID {
		return false
	}
	if a.targetCompID != b.targetCompID {
		return false
	}
	if a.side != b.side {
		return false
	}
	if a.ordType != b.ordType {
		return false
	}
	if !a.price.Equal(b.price) {
		return false
	}
	if !a.quantity.Equal(b.quantity) {
		return false
	}
	if !a.executedQuantity.Equal(b.executedQuantity) {
		return false
	}
	if !a.leavesQty.Equal(b.leavesQty) {
		return false
	}
	if !a.lastExecQuantity.Equal(b.lastExecQuantity) {
		return false
	}
	if !a.lastExecPx.Equal(b.lastExecPx) {
		return false
	}
	if !a.executedNotional.Equal(b.executedNotional) {
		return false
	}
	if a.status != b.status {
		return false
	}
	return true
}
