package topic

import (
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"kafka-bench-v4/internal/config"
)

func TestEnsureWithAdminCreatesMissingTopic(t *testing.T) {
	admin := newFakeAdmin()
	cfg := topicTestConfig()

	if err := ensureWithAdmin(cfg, admin, func(time.Duration) {}); err != nil {
		t.Fatalf("ensureWithAdmin error = %v", err)
	}
	if admin.created != 1 || admin.deleted != 0 {
		t.Fatalf("created/deleted = %d/%d", admin.created, admin.deleted)
	}
	if detail := admin.topics[cfg.Topic]; detail.NumPartitions != int32(cfg.Partitions) || detail.ReplicationFactor != 1 {
		t.Fatalf("topic detail = %+v", detail)
	}
}

func TestEnsureWithAdminReusesExistingTopic(t *testing.T) {
	cfg := topicTestConfig()
	cfg.ResetTopic = false
	admin := newFakeAdmin()
	admin.topics[cfg.Topic] = sarama.TopicDetail{NumPartitions: 1}

	if err := ensureWithAdmin(cfg, admin, func(time.Duration) {}); err != nil {
		t.Fatalf("ensureWithAdmin error = %v", err)
	}
	if admin.created != 0 || admin.deleted != 0 {
		t.Fatalf("created/deleted = %d/%d", admin.created, admin.deleted)
	}
}

func TestEnsureWithAdminResetsExistingTopic(t *testing.T) {
	cfg := topicTestConfig()
	cfg.ResetTopic = true
	admin := newFakeAdmin()
	admin.topics[cfg.Topic] = sarama.TopicDetail{NumPartitions: 1}
	var slept time.Duration

	if err := ensureWithAdmin(cfg, admin, func(d time.Duration) { slept = d }); err != nil {
		t.Fatalf("ensureWithAdmin error = %v", err)
	}
	if admin.deleted != 1 || admin.created != 1 {
		t.Fatalf("deleted/created = %d/%d", admin.deleted, admin.created)
	}
	if slept != topicDeletionDelay {
		t.Fatalf("slept = %v, want %v", slept, topicDeletionDelay)
	}
}

func TestEnsureWithAdminPropagatesErrors(t *testing.T) {
	tests := []struct {
		name  string
		admin *fakeAdmin
	}{
		{name: "list", admin: &fakeAdmin{listErr: errors.New("bad list"), topics: map[string]sarama.TopicDetail{}}},
		{name: "delete", admin: &fakeAdmin{deleteErr: errors.New("bad delete"), topics: map[string]sarama.TopicDetail{"topic": {}}}},
		{name: "create", admin: &fakeAdmin{createErr: errors.New("bad create"), topics: map[string]sarama.TopicDetail{}}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := ensureWithAdmin(topicTestConfig(), tt.admin, func(time.Duration) {})
			if err == nil {
				t.Fatal("ensureWithAdmin error = nil")
			}
			if !errors.Is(err, tt.admin.listErr) && !errors.Is(err, tt.admin.deleteErr) && !errors.Is(err, tt.admin.createErr) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func topicTestConfig() *config.BenchConfig {
	return &config.BenchConfig{
		Topic:      "topic",
		Partitions: 3,
		ResetTopic: true,
	}
}

type fakeAdmin struct {
	topics    map[string]sarama.TopicDetail
	listErr   error
	deleteErr error
	createErr error
	created   int
	deleted   int
}

func newFakeAdmin() *fakeAdmin {
	return &fakeAdmin{topics: map[string]sarama.TopicDetail{}}
}

func (f *fakeAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make(map[string]sarama.TopicDetail, len(f.topics))
	for k, v := range f.topics {
		out[k] = v
	}
	return out, nil
}

func (f *fakeAdmin) DeleteTopic(topic string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deleted++
	delete(f.topics, topic)
	return nil
}

func (f *fakeAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, _ bool) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.created++
	f.topics[topic] = *detail
	return nil
}

func (f *fakeAdmin) Close() error { return nil }
