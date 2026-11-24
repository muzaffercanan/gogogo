// Package worker delivers notifications from the work queues through the provider.
package worker

import (
	"context"
	"fmt"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/queue"
)

// Weights are the shares of the priorities while all of them have messages waiting. Each is at least 1,
// so no priority starves (ADR 0002).
type Weights struct {
	High, Normal, Low int
}

// DefaultWeights returns 6:3:1: under contention high gets 60% of a channel's sends, normal 30% and
// low 10%.
func DefaultWeights() Weights {
	return Weights{High: 6, Normal: 3, Low: 1}
}

func (w Weights) of(p domain.Priority) int {
	switch p {
	case domain.PriorityHigh:
		return w.High
	case domain.PriorityNormal:
		return w.Normal
	case domain.PriorityLow:
		return w.Low
	default:
		return 0
	}
}

// Source holds the delivery buffers of one channel's work queues. *queue.Consumer is one.
type Source interface {
	Deliveries(p domain.Priority) <-chan queue.Delivery
}

// Picker decides which delivery of a channel goes next, with a work-conserving smooth weighted
// round-robin over the priorities (ADR 0002, K-36). Each pick adds every ready priority's weight to its
// current value, takes the priority with the highest value (the higher priority on a tie) and subtracts
// the ready priorities' total weight from it. A priority is ready while the picker holds one of its
// deliveries; one whose buffer is empty sits out with its value kept, and the others share its turns.
// With 6:3:1 and every priority ready, ten picks go H N H H N H L H N H.
//
// A Picker holds at most one delivery per priority taken from its buffer. It is not safe for
// concurrent use: one dispatcher per channel calls Next, before taking a rate-limit token for the
// delivery it returns.
type Picker struct {
	classes []class // in priority order, highest first
}

type class struct {
	priority domain.Priority
	weight   int
	current  int
	in       <-chan queue.Delivery

	held bool
	head queue.Delivery
}

// NewPicker returns a picker over the buffers of src with the given weights.
func NewPicker(src Source, w Weights) (*Picker, error) {
	priorities := queue.Priorities()
	p := &Picker{classes: make([]class, 0, len(priorities))}
	for _, pr := range priorities {
		weight := w.of(pr)
		if weight < 1 {
			return nil, fmt.Errorf("weight of %s priority is %d, want at least 1", pr, weight)
		}
		in := src.Deliveries(pr)
		if in == nil {
			return nil, fmt.Errorf("no deliveries for %s priority", pr)
		}
		p.classes = append(p.classes, class{priority: pr, weight: weight, in: in})
	}
	return p, nil
}

// Next returns the next delivery, waiting until one arrives if every buffer is empty. It returns
// ctx.Err() once ctx has ended; deliveries it still holds then stay unacknowledged and go back to their
// queues when the consumer's channel closes.
func (p *Picker) Next(ctx context.Context) (queue.Delivery, error) {
	for {
		if err := ctx.Err(); err != nil {
			return queue.Delivery{}, err
		}
		p.fill()
		if d, ok := p.pick(); ok {
			return d, nil
		}
		// Every buffer is empty: take whatever arrives first, then pick again in case more arrived
		// meanwhile. The select below covers the three priorities of queue.Priorities.
		var (
			d queue.Delivery
			i int
		)
		select {
		case <-ctx.Done():
			return queue.Delivery{}, ctx.Err()
		case d = <-p.classes[0].in:
			i = 0
		case d = <-p.classes[1].in:
			i = 1
		case d = <-p.classes[2].in:
			i = 2
		}
		p.classes[i].held, p.classes[i].head = true, d
	}
}

// fill takes one delivery from the buffer of every priority the picker holds none of, without waiting.
func (p *Picker) fill() {
	for i := range p.classes {
		c := &p.classes[i]
		if c.held {
			continue
		}
		select {
		case d := <-c.in:
			c.held, c.head = true, d
		default:
		}
	}
}

// pick runs one round of the smooth weighted round-robin over the ready priorities and hands out the
// winner's delivery. It reports false when no priority is ready.
func (p *Picker) pick() (queue.Delivery, bool) {
	total, best := 0, -1
	for i := range p.classes {
		c := &p.classes[i]
		if !c.held {
			continue
		}
		c.current += c.weight
		total += c.weight
		if best < 0 || c.current > p.classes[best].current {
			best = i
		}
	}
	if best < 0 {
		return queue.Delivery{}, false
	}
	c := &p.classes[best]
	c.current -= total
	d := c.head
	c.held, c.head = false, queue.Delivery{}
	return d, true
}
