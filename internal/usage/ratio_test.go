package usage

import "testing"

func TestRatioDefaultsToOne(t *testing.T) {
	r := New()
	if got := r.Get("glm-5.3"); got != 1.0 {
		t.Errorf("unobserved model ratio = %v, want 1.0", got)
	}
	if got := r.Scaled("glm-5.3", 106228); got != 106228 {
		t.Errorf("Scaled on unobserved model = %d, want identity", got)
	}
}

func TestRatioFirstObservationSets(t *testing.T) {
	r := New()
	r.Observe("glm-5.3", 106228, 163049) // the 2026-10-18 dump measurement
	want := 163049.0 / 106228.0
	if got := r.Get("glm-5.3"); got != want {
		t.Errorf("ratio = %v, want %v", got, want)
	}
	if got := r.Scaled("glm-5.3", 106228); got != 163049 {
		t.Errorf("Scaled = %d, want 163049 (rounded)", got)
	}
	if got := r.Get("deepseek-v4"); got != 1.0 {
		t.Errorf("other model ratio = %v, want untouched 1.0", got)
	}
}

func TestRatioBlendsEMA(t *testing.T) {
	r := New()
	r.Observe("m", 1000, 1500) // ratio 1.5
	r.Observe("m", 1000, 2500) // obs 2.5 → (1.5+2.5)/2 = 2.0
	if got := r.Get("m"); got != 2.0 {
		t.Errorf("blended ratio = %v, want 2.0", got)
	}
}

func TestRatioDropsGarbageObservations(t *testing.T) {
	r := New()
	r.Observe("m", 999, 5000)  // estimate below floor
	r.Observe("m", 2000, 0)    // no real tokens
	r.Observe("m", 2000, -5)   // negative
	r.Observe("m", 1000, 100)  // ratio 0.1 < minRatio
	r.Observe("m", 1000, 9999) // ratio ~10 > maxRatio
	if got := r.Get("m"); got != 1.0 {
		t.Errorf("ratio = %v, want 1.0 (all observations dropped)", got)
	}
	// Boundary ratios within the clamp are accepted.
	r.Observe("m", 1000, 250)
	if got := r.Get("m"); got != 0.25 {
		t.Errorf("minRatio boundary = %v, want 0.25", got)
	}
}

func TestScaledRoundsAndGuards(t *testing.T) {
	r := New()
	r.Observe("m", 1000, 1500) // 1.5
	if got := r.Scaled("m", 3); got != 5 { // 4.5 → 5
		t.Errorf("Scaled(3) = %d, want 5", got)
	}
	if got := r.Scaled("m", 0); got != 0 {
		t.Errorf("Scaled(0) = %d, want 0", got)
	}
	if got := r.Scaled("m", -1); got != -1 {
		t.Errorf("Scaled(-1) = %d, want -1 (returned as-is)", got)
	}
}
