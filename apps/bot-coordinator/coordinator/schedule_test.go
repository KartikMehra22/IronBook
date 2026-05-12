package coordinator

import "testing"

func TestCompile_DeterministicForSameSeed(t *testing.T) {
	a := Compile("", 42, 1)
	b := Compile("", 42, 1)
	if len(a) != 1000 {
		t.Fatalf("expected 1000 events for 1s, got %d", len(a))
	}
	if ScheduleHash(a) != ScheduleHash(b) {
		t.Fatal("hash differs across compiles with same seed")
	}
}

func TestCompile_DifferentSeedDifferentHash(t *testing.T) {
	a := Compile("", 1, 1)
	b := Compile("", 2, 1)
	if ScheduleHash(a) == ScheduleHash(b) {
		t.Fatal("hash collides across different seeds")
	}
}

func TestCompile_DurationZero(t *testing.T) {
	if got := Compile("", 1, 0); len(got) != 0 {
		t.Fatalf("duration 0 should produce no events; got %d", len(got))
	}
}

func TestCompile_OffsetMonotonic(t *testing.T) {
	ev := Compile("", 7, 2)
	for i := 1; i < len(ev); i++ {
		if ev[i].OffsetNs <= ev[i-1].OffsetNs {
			t.Fatalf("offset %d not monotonic: prev=%d cur=%d", i, ev[i-1].OffsetNs, ev[i].OffsetNs)
		}
	}
}
