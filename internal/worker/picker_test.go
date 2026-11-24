package worker_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/worker"
)

var _ worker.Source = (*queue.Consumer)(nil)

// buffers is a Source whose buffers the test fills by hand.
type buffers map[domain.Priority]chan queue.Delivery

func newBuffers() buffers {
	b := buffers{}
	for _, p := range queue.Priorities() {
		b[p] = make(chan queue.Delivery, 1000)
	}
	return b
}

func (b buffers) Deliveries(p domain.Priority) <-chan queue.Delivery { return b[p] }

// add puts n deliveries of priority p into its buffer, numbered from how many the buffer already holds.
func (b buffers) add(p domain.Priority, n int) {
	for range n {
		b[p] <- queue.Delivery{NotificationID: fmt.Sprintf("%s-%d", p, len(b[p])), Channel: domain.ChannelSMS, Priority: p}
	}
}

func newPicker(t *testing.T, b buffers) *worker.Picker {
	t.Helper()
	p, err := worker.NewPicker(b, worker.DefaultWeights())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// letter abbreviates a priority to H, N or L.
func letter(p domain.Priority) string { return strings.ToUpper(string(p[0])) }

// pick takes n deliveries and returns their priorities as letters, for example "HNH".
func pick(t *testing.T, p *worker.Picker, n int) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var sb strings.Builder
	for range n {
		d, err := p.Next(ctx)
		if err != nil {
			t.Fatalf("Next after %q: %v", sb.String(), err)
		}
		sb.WriteString(letter(d.Priority))
	}
	return sb.String()
}

func TestPickerInterleavesPrioritiesSmoothlyAtSixThreeOne(t *testing.T) {
	b := newBuffers()
	for _, p := range queue.Priorities() {
		b.add(p, 200)
	}
	picker := newPicker(t, b)

	// Each round of ten picks spreads the six high, three normal and one low evenly, so low never waits
	// more than nine picks and normal never more than three.
	for round := range 10 {
		if got := pick(t, picker, 10); got != "HNHHNHLHNH" {
			t.Fatalf("round %d picked %s, want HNHHNHLHNH", round, got)
		}
	}
}

func TestPickerGivesAnEmptyPrioritysTurnsToTheOthers(t *testing.T) {
	tests := []struct {
		ready    string // priorities with a backlog
		picks    int    // a whole number of rounds
		wantH    int
		wantN    int
		wantL    int
		wantHead string // the first round
	}{
		{ready: "HN", picks: 90, wantH: 60, wantN: 30, wantHead: "HNHHNHHNH"},
		{ready: "HL", picks: 70, wantH: 60, wantL: 10, wantHead: "HHHLHHH"},
		{ready: "NL", picks: 40, wantN: 30, wantL: 10, wantHead: "NNLN"},
		{ready: "H", picks: 10, wantH: 10, wantHead: "H"},
		{ready: "N", picks: 10, wantN: 10, wantHead: "N"},
		{ready: "L", picks: 10, wantL: 10, wantHead: "L"},
	}
	for _, tt := range tests {
		t.Run(tt.ready, func(t *testing.T) {
			b := newBuffers()
			for _, p := range queue.Priorities() {
				if strings.Contains(tt.ready, letter(p)) {
					b.add(p, 200)
				}
			}
			got := pick(t, newPicker(t, b), tt.picks)
			if !strings.HasPrefix(got, tt.wantHead) {
				t.Errorf("first picks %s, want %s", got[:len(tt.wantHead)], tt.wantHead)
			}
			h, n, l := strings.Count(got, "H"), strings.Count(got, "N"), strings.Count(got, "L")
			if h != tt.wantH || n != tt.wantN || l != tt.wantL {
				t.Errorf("%d picks gave high %d, normal %d, low %d; want %d, %d, %d", tt.picks, h, n, l, tt.wantH, tt.wantN, tt.wantL)
			}
		})
	}
}

func TestPickerKeepsTheTurnOfAPriorityWhoseBufferRanEmpty(t *testing.T) {
	b := newBuffers()
	b.add(domain.PriorityHigh, 1)
	b.add(domain.PriorityNormal, 20)
	b.add(domain.PriorityLow, 20)
	picker := newPicker(t, b)

	// High is picked first and then has nothing; normal and low share its turns.
	if got := pick(t, picker, 3); got != "HNN" {
		t.Fatalf("first picks %s, want HNN", got)
	}
	// High comes back owing for its first pick, so it waits one more turn. Had its place been reset
	// while it sat out, it would go at once.
	b.add(domain.PriorityHigh, 20)
	if got := pick(t, picker, 2); got != "NH" {
		t.Errorf("picks after high came back %s, want NH", got)
	}
}

func TestPickerKeepsArrivalOrderWithinAPriority(t *testing.T) {
	b := newBuffers()
	for _, p := range queue.Priorities() {
		b.add(p, 30)
	}
	picker := newPicker(t, b)
	next := map[domain.Priority]int{}
	for range 50 {
		d, err := picker.Next(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if want := fmt.Sprintf("%s-%d", d.Priority, next[d.Priority]); d.NotificationID != want {
			t.Fatalf("got %s, want %s", d.NotificationID, want)
		}
		next[d.Priority]++
	}
}

func TestPickerWaitsForTheFirstDeliveryWhenEveryBufferIsEmpty(t *testing.T) {
	b := newBuffers()
	picker := newPicker(t, b)

	got := make(chan queue.Delivery, 1)
	go func() {
		d, err := picker.Next(t.Context())
		if err != nil {
			t.Errorf("Next: %v", err)
		}
		got <- d
	}()
	select {
	case d := <-got:
		t.Fatalf("Next returned %s with every buffer empty", d.NotificationID)
	case <-time.After(100 * time.Millisecond):
	}

	b.add(domain.PriorityLow, 1)
	select {
	case d := <-got:
		if d.NotificationID != "low-0" {
			t.Errorf("Next returned %s, want low-0", d.NotificationID)
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not return after a delivery arrived")
	}
}

func TestPickerStopsWhenItsContextEnds(t *testing.T) {
	t.Run("while waiting", func(t *testing.T) {
		picker := newPicker(t, newBuffers())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := picker.Next(ctx)
			done <- err
		}()
		time.Sleep(50 * time.Millisecond)
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Next = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Next did not return after its context was cancelled")
		}
	})
	t.Run("with deliveries waiting", func(t *testing.T) {
		b := newBuffers()
		b.add(domain.PriorityHigh, 1)
		picker := newPicker(t, b)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if d, err := picker.Next(ctx); !errors.Is(err, context.Canceled) {
			t.Errorf("Next = %s, %v; want context.Canceled", d.NotificationID, err)
		}
	})
}

func TestNewPickerRejectsWeightsBelowOneAndMissingBuffers(t *testing.T) {
	tests := []struct {
		name    string
		weights worker.Weights
		missing domain.Priority
		wantErr bool
	}{
		{name: "default weights", weights: worker.DefaultWeights()},
		{name: "equal weights", weights: worker.Weights{High: 1, Normal: 1, Low: 1}},
		{name: "zero low weight", weights: worker.Weights{High: 6, Normal: 3, Low: 0}, wantErr: true},
		{name: "negative high weight", weights: worker.Weights{High: -1, Normal: 3, Low: 1}, wantErr: true},
		{name: "unset weights", weights: worker.Weights{}, wantErr: true},
		{name: "no normal buffer", weights: worker.DefaultWeights(), missing: domain.PriorityNormal, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBuffers()
			if tt.missing != "" {
				delete(b, tt.missing)
			}
			_, err := worker.NewPicker(b, tt.weights)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewPicker error = %v, want error %t", err, tt.wantErr)
			}
		})
	}
}
