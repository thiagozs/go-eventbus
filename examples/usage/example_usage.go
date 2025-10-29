package main

import (
	"context"
	"fmt"
	"time"

	"github.com/thiagozs/go-eventbus"
)

type UserCreatedEvent struct {
	UserID string
	Email  string
	Name   string
}

type UserUpdatedEvent struct {
	UserID string
	Action string
}

type OrderPlacedEvent struct {
	OrderID string
	UserID  string
	Total   float64
	Items   []string
}

type PaymentProcessedEvent struct {
	PaymentID string
	OrderID   string
	Amount    float64
	Method    string
}

func main() {
	// Configuração do EventBus
	config := eventbus.Config{
		Workers:   5,
		QueueSize: 50,
		Retries:   3,
		DLQSize:   20,
	}

	bus := eventbus.NewEventBus(config)

	// Registra handlers TIPADOS para diferentes eventos
	eventbus.Subscribe(bus, "user.created", handleUserCreated)
	eventbus.Subscribe(bus, "user.updated", handleUserUpdated)
	eventbus.Subscribe(bus, "order.placed", handleOrderPlaced)
	eventbus.Subscribe(bus, "payment.processed", handlePayment)

	// Monitora a Dead Letter Queue
	go monitorDLQ(bus)

	fmt.Println("🚀 Iniciando EventBus...")
	fmt.Println()

	// Publica eventos com STRUCTS TIPADAS! 🎯
	eventbus.Publish(bus, "user.created", UserCreatedEvent{
		UserID: "123",
		Email:  "joao@example.com",
		Name:   "João Silva",
	})

	eventbus.Publish(bus, "order.placed", OrderPlacedEvent{
		OrderID: "ORD-001",
		UserID:  "123",
		Total:   150.50,
		Items:   []string{"Item A", "Item B"},
	})

	// Evento com prioridade alta
	eventbus.PublishWithPriority(bus, "payment.processed",
		PaymentProcessedEvent{
			PaymentID: "PAY-001",
			OrderID:   "ORD-001",
			Amount:    150.50,
			Method:    "credit_card",
		}, 10)

	// Múltiplos eventos
	for i := range 5 {
		eventbus.Publish(bus, "user.updated", UserUpdatedEvent{
			UserID: fmt.Sprintf("user-%d", i),
			Action: "profile_updated",
		})
	}

	// Aguarda processamento
	time.Sleep(3 * time.Second)

	// Estatísticas
	stats := bus.Stats()
	fmt.Println("\n📊 Estatísticas do EventBus:")
	for key, value := range stats {
		fmt.Printf("  %s: %v\n", key, value)
	}

	// Shutdown graceful
	fmt.Println("\n🛑 Encerrando EventBus...")
	if err := bus.Shutdown(5 * time.Second); err != nil {
		fmt.Printf("Erro ao encerrar: %v\n", err)
	}
}

// Handlers TIPADOS - O payload já vem no tipo correto! 🎉

func handleUserCreated(ctx context.Context,
	event eventbus.Event[UserCreatedEvent]) error {
	// Acesso direto aos campos, sem type assertion!
	fmt.Printf("✅ [USER.CREATED] Novo usuário: %s (%s)\n",
		event.Payload.Name, event.Payload.Email)
	time.Sleep(100 * time.Millisecond)
	return nil
}

func handleUserUpdated(ctx context.Context,
	event eventbus.Event[UserUpdatedEvent]) error {
	fmt.Printf("🔄 [USER.UPDATED] Usuário %s - Ação: %s\n",
		event.Payload.UserID, event.Payload.Action)
	time.Sleep(50 * time.Millisecond)
	return nil
}

func handleOrderPlaced(ctx context.Context,
	event eventbus.Event[OrderPlacedEvent]) error {
	fmt.Printf("🛒 [ORDER.PLACED] Pedido %s - Total: R$ %.2f - Itens: %v\n",
		event.Payload.OrderID, event.Payload.Total, event.Payload.Items)
	time.Sleep(200 * time.Millisecond)
	return nil
}

func handlePayment(ctx context.Context,
	event eventbus.Event[PaymentProcessedEvent]) error {
	fmt.Printf("💰 [PAYMENT.PROCESSED] Pagamento %s - R$ %.2f via %s\n",
		event.Payload.PaymentID, event.Payload.Amount, event.Payload.Method)
	time.Sleep(150 * time.Millisecond)
	return nil
}

// Monitor para Dead Letter Queue
func monitorDLQ(bus *eventbus.EventBus) {
	dlq := bus.GetDLQ()
	for wrapper := range dlq {
		dlqEvent := eventbus.GetDLQEvent(wrapper)
		fmt.Printf("⚠️  [DLQ] Evento falhou: %s - %+v\n",
			dlqEvent.Type, dlqEvent.Payload)
	}
}
