package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/thiagozs/go-eventbus"
)

type OrderCreatedEvent struct {
	OrderID     string
	CustomerID  string
	TotalAmount float64
	Items       []OrderItem
	CreatedAt   time.Time
}

type OrderItem struct {
	ProductID string
	Quantity  int
	Price     float64
}

type PaymentProcessedEvent struct {
	PaymentID     string
	OrderID       string
	Amount        float64
	PaymentMethod string
	Status        string
	ProcessedAt   time.Time
}

type InventoryUpdatedEvent struct {
	ProductID     string
	QuantityDelta int // Positivo = adicionar, Negativo = remover
	NewStock      int
	Warehouse     string
}

type EmailEvent struct {
	To      string
	Subject string
	Body    string
	Type    string // "welcome", "order_confirmation", etc
}

func main() {
	fmt.Println("🎯 EventBus com Generics - Exemplo Completo")

	// Configuração
	config := eventbus.Config{
		Workers:   3,
		QueueSize: 20,
		Retries:   2,
	}

	bus := eventbus.NewEventBus(config)

	//Registra handlers TYPE-SAFE
	// Handler de pedidos - recebe OrderCreatedEvent tipado!
	eventbus.Subscribe(bus, "order.created", func(ctx context.Context,
		event eventbus.Event[OrderCreatedEvent]) error {
		order := event.Payload
		fmt.Printf("📦 [ORDER] Novo pedido #%s\n", order.OrderID)
		fmt.Printf("   Cliente: %s\n", order.CustomerID)
		fmt.Printf("   Total: R$ %.2f\n", order.TotalAmount)
		fmt.Printf("   Itens: %d\n", len(order.Items))

		// Acesso type-safe aos items!
		for _, item := range order.Items {
			fmt.Printf("   - %s: %dx R$ %.2f\n", item.ProductID,
				item.Quantity, item.Price)
		}
		return nil
	})

	// Handler de pagamentos - recebe PaymentProcessedEvent tipado!
	eventbus.Subscribe(bus, "payment.processed", func(ctx context.Context,
		event eventbus.Event[PaymentProcessedEvent]) error {
		payment := event.Payload
		fmt.Printf("\n💰 [PAYMENT] Pagamento #%s processado\n", payment.PaymentID)
		fmt.Printf("   Pedido: %s\n", payment.OrderID)
		fmt.Printf("   Valor: R$ %.2f via %s\n", payment.Amount, payment.PaymentMethod)
		fmt.Printf("   Status: %s\n", payment.Status)
		return nil
	})

	// Handler de inventário - recebe InventoryUpdatedEvent tipado!
	eventbus.Subscribe(bus, "inventory.updated", func(ctx context.Context,
		event eventbus.Event[InventoryUpdatedEvent]) error {
		inv := event.Payload
		action := "adicionado"
		if inv.QuantityDelta < 0 {
			action = "removido"
		}
		fmt.Printf("\n📊 [INVENTORY] Estoque %s\n", action)
		fmt.Printf("   Produto: %s\n", inv.ProductID)
		fmt.Printf("   Delta: %d unidades\n", inv.QuantityDelta)
		fmt.Printf("   Novo estoque: %d\n", inv.NewStock)
		fmt.Printf("   Depósito: %s\n", inv.Warehouse)
		return nil
	})

	// Handler de emails - recebe EmailEvent tipado!
	eventbus.Subscribe(bus, "email.send", func(ctx context.Context,
		event eventbus.Event[EmailEvent]) error {
		email := event.Payload
		fmt.Printf("\n📧 [EMAIL] Enviando email\n")
		fmt.Printf("   Para: %s\n", email.To)
		fmt.Printf("   Assunto: %s\n", email.Subject)
		fmt.Printf("   Tipo: %s\n", email.Type)
		return nil
	})

	fmt.Println("\nSimulando fluxo de pedido...")
	fmt.Println(strings.Repeat("=", 60))

	// 1. Pedido criado
	eventbus.Publish(bus, "order.created", OrderCreatedEvent{
		OrderID:     "ORD-2024-001",
		CustomerID:  "CUST-123",
		TotalAmount: 299.90,
		Items: []OrderItem{
			{ProductID: "PROD-A", Quantity: 2, Price: 99.95},
			{ProductID: "PROD-B", Quantity: 1, Price: 100.00},
		},
		CreatedAt: time.Now(),
	})

	time.Sleep(500 * time.Millisecond)

	// 2. Atualiza inventário
	eventbus.Publish(bus, "inventory.updated", InventoryUpdatedEvent{
		ProductID:     "PROD-A",
		QuantityDelta: -2,
		NewStock:      48,
		Warehouse:     "SP-Central",
	})

	eventbus.Publish(bus, "inventory.updated", InventoryUpdatedEvent{
		ProductID:     "PROD-B",
		QuantityDelta: -1,
		NewStock:      15,
		Warehouse:     "SP-Central",
	})

	time.Sleep(500 * time.Millisecond)

	// 3. Pagamento processado
	eventbus.PublishWithPriority(bus, "payment.processed", PaymentProcessedEvent{
		PaymentID:     "PAY-2024-001",
		OrderID:       "ORD-2024-001",
		Amount:        299.90,
		PaymentMethod: "credit_card",
		Status:        "approved",
		ProcessedAt:   time.Now(),
	}, 10) // Alta prioridade!

	time.Sleep(500 * time.Millisecond)

	// 4. Envia email de confirmação
	eventbus.Publish(bus, "email.send", EmailEvent{
		To:      "cliente@example.com",
		Subject: "Pedido Confirmado - #ORD-2024-001",
		Body:    "Seu pedido foi confirmado e está sendo preparado!",
		Type:    "order_confirmation",
	})

	// Aguarda processar tudo
	time.Sleep(2 * time.Second)

	// Estatísticas
	fmt.Println("\n" + strings.Repeat("=", 60))
	stats := bus.Stats()
	fmt.Println("\n📊 Estatísticas do EventBus:")
	for key, value := range stats {
		fmt.Printf("   %s: %v\n", key, value)
	}

	// Shutdown
	fmt.Println("\n🛑 Encerrando...")
	bus.Shutdown(3 * time.Second)

	fmt.Println("\n✅ Concluído!")
	fmt.Println("\n💡 Vantagens dos Generics:")
	fmt.Println("   ✓ Type-safe: sem type assertions!")
	fmt.Println("   ✓ Autocomplete: IDE conhece os campos")
	fmt.Println("   ✓ Compile-time: erros detectados antes de rodar")
	fmt.Println("   ✓ Performance: zero overhead de reflexão")
	fmt.Println("   ✓ Legibilidade: código mais limpo e claro")
}
