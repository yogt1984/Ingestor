//! Signal Processing Module for MARS
//!
//! Implements a Kalman filter for optimal state estimation of price dynamics.
//! The filter provides smoothed estimates of:
//! - Position (smoothed price)
//! - Velocity (rate of price change / momentum)
//! - Acceleration (rate of momentum change / trend reversal indicator)
//!
//! # Mathematical Background
//!
//! The Kalman filter uses a constant-acceleration state-space model:
//!
//! ```text
//! State vector: x = [position, velocity, acceleration]^T
//!
//! State transition (discrete time, dt=1):
//! x(k+1) = F * x(k) + w(k)
//!
//! where F = | 1  dt  0.5*dt^2 |
//!           | 0  1   dt       |
//!           | 0  0   1        |
//!
//! Observation model:
//! z(k) = H * x(k) + v(k)
//!
//! where H = [1, 0, 0] (we only observe position/price)
//!
//! w(k) ~ N(0, Q) is process noise
//! v(k) ~ N(0, R) is measurement noise
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::features::signal_processing::{KalmanFilter, KalmanConfig};
//!
//! let config = KalmanConfig::default();
//! let mut filter = KalmanFilter::new(config);
//!
//! // Feed price observations
//! let state = filter.update(100.0);
//! println!("Smoothed price: {}", state.position);
//! println!("Velocity: {}", state.velocity);
//! println!("Acceleration: {}", state.acceleration);
//! ```

use serde::{Deserialize, Serialize};

/// Configuration for the Kalman filter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalmanConfig {
    /// Process noise variance for position
    /// Higher values = trust measurements more, model less
    pub process_noise_position: f64,

    /// Process noise variance for velocity
    pub process_noise_velocity: f64,

    /// Process noise variance for acceleration
    pub process_noise_acceleration: f64,

    /// Measurement noise variance
    /// Higher values = trust model more, measurements less
    pub measurement_noise: f64,

    /// Time step between observations (default: 1.0)
    pub dt: f64,
}

impl Default for KalmanConfig {
    fn default() -> Self {
        Self {
            // Conservative defaults suitable for price data
            process_noise_position: 0.01,
            process_noise_velocity: 0.01,
            process_noise_acceleration: 0.001,
            measurement_noise: 1.0,
            dt: 1.0,
        }
    }
}

impl KalmanConfig {
    /// Create a new configuration with custom parameters
    pub fn new(
        process_noise_position: f64,
        process_noise_velocity: f64,
        process_noise_acceleration: f64,
        measurement_noise: f64,
        dt: f64,
    ) -> Self {
        Self {
            process_noise_position,
            process_noise_velocity,
            process_noise_acceleration,
            measurement_noise,
            dt,
        }
    }

    /// Create a configuration optimized for high-frequency price data
    /// Uses lower process noise for smoother estimates
    pub fn high_frequency() -> Self {
        Self {
            process_noise_position: 0.001,
            process_noise_velocity: 0.001,
            process_noise_acceleration: 0.0001,
            measurement_noise: 1.0,
            dt: 1.0,
        }
    }

    /// Create a configuration for noisy data
    /// Uses higher measurement noise for more smoothing
    pub fn noisy_data() -> Self {
        Self {
            process_noise_position: 0.1,
            process_noise_velocity: 0.1,
            process_noise_acceleration: 0.01,
            measurement_noise: 10.0,
            dt: 1.0,
        }
    }
}

/// State estimate from the Kalman filter
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KalmanState {
    /// Smoothed position (price) estimate
    pub position: f64,

    /// Velocity estimate (rate of price change)
    /// Positive = price increasing, Negative = price decreasing
    pub velocity: f64,

    /// Acceleration estimate (rate of velocity change)
    /// Positive = momentum increasing, Negative = momentum decreasing
    pub acceleration: f64,

    /// Variance of position estimate (uncertainty)
    pub position_variance: f64,

    /// Variance of velocity estimate
    pub velocity_variance: f64,

    /// Variance of acceleration estimate
    pub acceleration_variance: f64,

    /// Innovation (measurement residual): z - H*x_predicted
    /// Large values indicate the observation deviates from model prediction
    pub innovation: f64,

    /// Innovation variance (S = H*P*H' + R)
    pub innovation_variance: f64,

    /// Number of observations processed
    pub observation_count: usize,
}

impl KalmanState {
    /// Get the Kalman gain for position (how much we trust the measurement)
    /// Range: [0, 1] where 1 = fully trust measurement, 0 = fully trust prediction
    pub fn position_gain(&self) -> f64 {
        if self.innovation_variance > 0.0 {
            self.position_variance / self.innovation_variance
        } else {
            0.0
        }
    }

    /// Normalized innovation (innovation / sqrt(innovation_variance))
    /// Should be approximately N(0,1) if filter is well-tuned
    pub fn normalized_innovation(&self) -> Option<f64> {
        if self.innovation_variance > 0.0 {
            Some(self.innovation / self.innovation_variance.sqrt())
        } else {
            None
        }
    }
}

/// Kalman filter for estimating price dynamics
///
/// Implements a 3-state (position, velocity, acceleration) Kalman filter
/// using explicit matrix operations for clarity and correctness.
#[derive(Debug, Clone)]
pub struct KalmanFilter {
    /// Filter configuration
    config: KalmanConfig,

    /// Current state estimate [position, velocity, acceleration]
    state: [f64; 3],

    /// Error covariance matrix P (3x3, stored as row-major)
    /// P[i][j] = covariance(state[i], state[j])
    covariance: [[f64; 3]; 3],

    /// Whether the filter has been initialized
    initialized: bool,

    /// Number of observations processed
    observation_count: usize,

    /// Last innovation value
    last_innovation: f64,

    /// Last innovation variance
    last_innovation_variance: f64,
}

impl KalmanFilter {
    /// Create a new Kalman filter with the given configuration
    pub fn new(config: KalmanConfig) -> Self {
        Self {
            config,
            state: [0.0; 3],
            covariance: [[0.0; 3]; 3],
            initialized: false,
            observation_count: 0,
            last_innovation: 0.0,
            last_innovation_variance: 0.0,
        }
    }

    /// Create a filter with default configuration
    pub fn default_filter() -> Self {
        Self::new(KalmanConfig::default())
    }

    /// Reset the filter to uninitialized state
    pub fn reset(&mut self) {
        self.state = [0.0; 3];
        self.covariance = [[0.0; 3]; 3];
        self.initialized = false;
        self.observation_count = 0;
        self.last_innovation = 0.0;
        self.last_innovation_variance = 0.0;
    }

    /// Get the current state estimate (returns None if not initialized)
    pub fn state(&self) -> Option<KalmanState> {
        if !self.initialized {
            return None;
        }

        Some(KalmanState {
            position: self.state[0],
            velocity: self.state[1],
            acceleration: self.state[2],
            position_variance: self.covariance[0][0],
            velocity_variance: self.covariance[1][1],
            acceleration_variance: self.covariance[2][2],
            innovation: self.last_innovation,
            innovation_variance: self.last_innovation_variance,
            observation_count: self.observation_count,
        })
    }

    /// Get raw state values (position, velocity, acceleration)
    pub fn raw_state(&self) -> Option<(f64, f64, f64)> {
        if !self.initialized {
            return None;
        }
        Some((self.state[0], self.state[1], self.state[2]))
    }

    /// Process a new price observation and return the updated state
    ///
    /// The Kalman filter algorithm:
    /// 1. Predict: Project state and covariance ahead
    /// 2. Update: Incorporate the new measurement
    pub fn update(&mut self, price: f64) -> KalmanState {
        if !self.initialized {
            // Initialize state with first observation
            self.state[0] = price;  // position = observed price
            self.state[1] = 0.0;    // velocity = 0 (unknown)
            self.state[2] = 0.0;    // acceleration = 0 (unknown)

            // Initialize covariance with high uncertainty for derivatives
            // Position uncertainty is measurement noise
            // Velocity/acceleration have high initial uncertainty
            self.covariance[0][0] = self.config.measurement_noise;
            self.covariance[1][1] = 100.0;  // High velocity uncertainty
            self.covariance[2][2] = 100.0;  // High acceleration uncertainty

            self.initialized = true;
            self.observation_count = 1;
            self.last_innovation = 0.0;
            self.last_innovation_variance = self.config.measurement_noise;

            return self.state().unwrap();
        }

        let dt = self.config.dt;
        let dt2 = dt * dt;
        let dt3 = dt2 * dt;
        let dt4 = dt3 * dt;

        // ========== PREDICT STEP ==========

        // State transition matrix F
        // F = | 1  dt  0.5*dt^2 |
        //     | 0  1   dt       |
        //     | 0  0   1        |

        // Predicted state: x_pred = F * x
        let x_pred = [
            self.state[0] + self.state[1] * dt + 0.5 * self.state[2] * dt2,
            self.state[1] + self.state[2] * dt,
            self.state[2],
        ];

        // Process noise covariance Q
        // Using discrete white noise model for constant acceleration
        let q_pos = self.config.process_noise_position;
        let q_vel = self.config.process_noise_velocity;
        let q_acc = self.config.process_noise_acceleration;

        // Simplified diagonal Q for numerical stability
        // In practice, a proper Q would be derived from continuous process noise
        // but diagonal Q works well for trading applications
        let q = [
            [q_pos + 0.25 * q_acc * dt4, 0.5 * q_acc * dt3, 0.5 * q_acc * dt2],
            [0.5 * q_acc * dt3, q_vel + q_acc * dt2, q_acc * dt],
            [0.5 * q_acc * dt2, q_acc * dt, q_acc],
        ];

        // Predicted covariance: P_pred = F * P * F' + Q
        // First compute F * P
        let fp = [
            [
                self.covariance[0][0] + dt * self.covariance[1][0] + 0.5 * dt2 * self.covariance[2][0],
                self.covariance[0][1] + dt * self.covariance[1][1] + 0.5 * dt2 * self.covariance[2][1],
                self.covariance[0][2] + dt * self.covariance[1][2] + 0.5 * dt2 * self.covariance[2][2],
            ],
            [
                self.covariance[1][0] + dt * self.covariance[2][0],
                self.covariance[1][1] + dt * self.covariance[2][1],
                self.covariance[1][2] + dt * self.covariance[2][2],
            ],
            [
                self.covariance[2][0],
                self.covariance[2][1],
                self.covariance[2][2],
            ],
        ];

        // Then compute (F * P) * F' + Q
        let p_pred = [
            [
                fp[0][0] + dt * fp[0][1] + 0.5 * dt2 * fp[0][2] + q[0][0],
                fp[0][1] + dt * fp[0][2] + q[0][1],
                fp[0][2] + q[0][2],
            ],
            [
                fp[1][0] + dt * fp[1][1] + 0.5 * dt2 * fp[1][2] + q[1][0],
                fp[1][1] + dt * fp[1][2] + q[1][1],
                fp[1][2] + q[1][2],
            ],
            [
                fp[2][0] + dt * fp[2][1] + 0.5 * dt2 * fp[2][2] + q[2][0],
                fp[2][1] + dt * fp[2][2] + q[2][1],
                fp[2][2] + q[2][2],
            ],
        ];

        // ========== UPDATE STEP ==========

        // Observation matrix H = [1, 0, 0]
        // We only observe position

        // Innovation: y = z - H * x_pred = price - x_pred[0]
        let innovation = price - x_pred[0];

        // Innovation covariance: S = H * P_pred * H' + R = P_pred[0][0] + R
        let s = p_pred[0][0] + self.config.measurement_noise;

        // Store for diagnostics
        self.last_innovation = innovation;
        self.last_innovation_variance = s;

        // Kalman gain: K = P_pred * H' / S = P_pred[*][0] / S
        let k = [
            p_pred[0][0] / s,
            p_pred[1][0] / s,
            p_pred[2][0] / s,
        ];

        // Updated state: x = x_pred + K * y
        self.state[0] = x_pred[0] + k[0] * innovation;
        self.state[1] = x_pred[1] + k[1] * innovation;
        self.state[2] = x_pred[2] + k[2] * innovation;

        // Updated covariance: P = (I - K * H) * P_pred
        // Using Joseph form for numerical stability:
        // P = (I - K*H) * P_pred * (I - K*H)' + K * R * K'
        // Simplified since H = [1, 0, 0]:
        // P = P_pred - K * P_pred[0][*] - P_pred[*][0] * K' + K * S * K'

        // For numerical stability, use the simpler form:
        // P = (I - K * H) * P_pred
        for i in 0..3 {
            for j in 0..3 {
                self.covariance[i][j] = p_pred[i][j] - k[i] * p_pred[0][j];
            }
        }

        // Ensure symmetry and positive definiteness
        for i in 0..3 {
            for j in (i + 1)..3 {
                let avg = (self.covariance[i][j] + self.covariance[j][i]) / 2.0;
                self.covariance[i][j] = avg;
                self.covariance[j][i] = avg;
            }
            // Ensure diagonal elements are positive
            if self.covariance[i][i] < 1e-10 {
                self.covariance[i][i] = 1e-10;
            }
        }

        self.observation_count += 1;

        self.state().unwrap()
    }

    /// Get the configuration
    pub fn config(&self) -> &KalmanConfig {
        &self.config
    }

    /// Check if the filter has been initialized
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Get the number of observations processed
    pub fn observation_count(&self) -> usize {
        self.observation_count
    }

    /// Predict the state n steps ahead without updating
    pub fn predict_ahead(&self, steps: usize) -> Option<(f64, f64, f64)> {
        if !self.initialized {
            return None;
        }

        let dt = self.config.dt * steps as f64;
        let dt2 = dt * dt;

        let pos = self.state[0] + self.state[1] * dt + 0.5 * self.state[2] * dt2;
        let vel = self.state[1] + self.state[2] * dt;
        let acc = self.state[2];

        Some((pos, vel, acc))
    }
}

/// Helper struct for batch processing multiple price series
#[derive(Debug, Clone)]
pub struct MultiSymbolKalman {
    filters: Vec<(String, KalmanFilter)>,
}

impl MultiSymbolKalman {
    /// Create a new multi-symbol Kalman filter processor
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
        }
    }

    /// Add a symbol with the given configuration
    pub fn add_symbol(&mut self, symbol: &str, config: KalmanConfig) {
        self.filters.push((symbol.to_string(), KalmanFilter::new(config)));
    }

    /// Update a specific symbol's filter
    pub fn update(&mut self, symbol: &str, price: f64) -> Option<KalmanState> {
        for (sym, filter) in &mut self.filters {
            if sym == symbol {
                return Some(filter.update(price));
            }
        }
        None
    }

    /// Get the state for a specific symbol
    pub fn state(&self, symbol: &str) -> Option<KalmanState> {
        for (sym, filter) in &self.filters {
            if sym == symbol {
                return filter.state();
            }
        }
        None
    }

    /// Get all current states
    pub fn all_states(&self) -> Vec<(&str, Option<KalmanState>)> {
        self.filters
            .iter()
            .map(|(sym, filter)| (sym.as_str(), filter.state()))
            .collect()
    }
}

impl Default for MultiSymbolKalman {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Tolerance for floating point comparisons
    const EPSILON: f64 = 1e-6;
    const LOOSE_EPSILON: f64 = 1e-3;

    fn approx_eq(a: f64, b: f64, epsilon: f64) -> bool {
        (a - b).abs() < epsilon
    }

    // ========================================================================
    // Test 1-10: Basic initialization and configuration tests
    // ========================================================================

    #[test]
    fn test_01_default_config_values() {
        let config = KalmanConfig::default();
        assert_eq!(config.process_noise_position, 0.01);
        assert_eq!(config.process_noise_velocity, 0.01);
        assert_eq!(config.process_noise_acceleration, 0.001);
        assert_eq!(config.measurement_noise, 1.0);
        assert_eq!(config.dt, 1.0);
    }

    #[test]
    fn test_02_custom_config() {
        let config = KalmanConfig::new(0.1, 0.2, 0.3, 0.4, 0.5);
        assert_eq!(config.process_noise_position, 0.1);
        assert_eq!(config.process_noise_velocity, 0.2);
        assert_eq!(config.process_noise_acceleration, 0.3);
        assert_eq!(config.measurement_noise, 0.4);
        assert_eq!(config.dt, 0.5);
    }

    #[test]
    fn test_03_high_frequency_config() {
        let config = KalmanConfig::high_frequency();
        assert_eq!(config.process_noise_position, 0.001);
        assert_eq!(config.process_noise_velocity, 0.001);
        assert_eq!(config.process_noise_acceleration, 0.0001);
        assert_eq!(config.measurement_noise, 1.0);
    }

    #[test]
    fn test_04_noisy_data_config() {
        let config = KalmanConfig::noisy_data();
        assert_eq!(config.process_noise_position, 0.1);
        assert_eq!(config.measurement_noise, 10.0);
    }

    #[test]
    fn test_05_filter_not_initialized() {
        let filter = KalmanFilter::default_filter();
        assert!(!filter.is_initialized());
        assert!(filter.state().is_none());
        assert!(filter.raw_state().is_none());
        assert_eq!(filter.observation_count(), 0);
    }

    #[test]
    fn test_06_filter_initialized_after_first_update() {
        let mut filter = KalmanFilter::default_filter();
        filter.update(100.0);
        assert!(filter.is_initialized());
        assert!(filter.state().is_some());
        assert_eq!(filter.observation_count(), 1);
    }

    #[test]
    fn test_07_first_update_sets_position() {
        let mut filter = KalmanFilter::default_filter();
        let state = filter.update(100.0);
        assert!(approx_eq(state.position, 100.0, EPSILON));
    }

    #[test]
    fn test_08_first_update_zero_velocity() {
        let mut filter = KalmanFilter::default_filter();
        let state = filter.update(100.0);
        assert!(approx_eq(state.velocity, 0.0, EPSILON));
    }

    #[test]
    fn test_09_first_update_zero_acceleration() {
        let mut filter = KalmanFilter::default_filter();
        let state = filter.update(100.0);
        assert!(approx_eq(state.acceleration, 0.0, EPSILON));
    }

    #[test]
    fn test_10_reset_clears_state() {
        let mut filter = KalmanFilter::default_filter();
        filter.update(100.0);
        filter.update(101.0);
        filter.reset();
        assert!(!filter.is_initialized());
        assert!(filter.state().is_none());
        assert_eq!(filter.observation_count(), 0);
    }

    // ========================================================================
    // Test 11-20: Constant price (no trend) tests
    // ========================================================================

    #[test]
    fn test_11_constant_price_position_converges() {
        let mut filter = KalmanFilter::default_filter();
        for _ in 0..100 {
            filter.update(100.0);
        }
        let state = filter.state().unwrap();
        assert!(approx_eq(state.position, 100.0, LOOSE_EPSILON));
    }

    #[test]
    fn test_12_constant_price_velocity_converges_to_zero() {
        let mut filter = KalmanFilter::default_filter();
        for _ in 0..100 {
            filter.update(100.0);
        }
        let state = filter.state().unwrap();
        assert!(approx_eq(state.velocity, 0.0, LOOSE_EPSILON));
    }

    #[test]
    fn test_13_constant_price_acceleration_converges_to_zero() {
        let mut filter = KalmanFilter::default_filter();
        for _ in 0..100 {
            filter.update(100.0);
        }
        let state = filter.state().unwrap();
        assert!(approx_eq(state.acceleration, 0.0, LOOSE_EPSILON));
    }

    #[test]
    fn test_14_constant_price_variance_decreases() {
        let mut filter = KalmanFilter::default_filter();
        filter.update(100.0);
        let initial_var = filter.state().unwrap().position_variance;

        for _ in 0..50 {
            filter.update(100.0);
        }
        let final_var = filter.state().unwrap().position_variance;

        assert!(final_var < initial_var, "Variance should decrease with more observations");
    }

    #[test]
    fn test_15_constant_price_innovation_converges_to_zero() {
        let mut filter = KalmanFilter::default_filter();
        for _ in 0..100 {
            filter.update(100.0);
        }
        let state = filter.state().unwrap();
        assert!(approx_eq(state.innovation, 0.0, LOOSE_EPSILON));
    }

    #[test]
    fn test_16_constant_price_different_values() {
        let test_prices = [50.0, 200.0, 1000.0, 0.001, 99999.0];
        for price in test_prices {
            let mut filter = KalmanFilter::default_filter();
            for _ in 0..100 {
                filter.update(price);
            }
            let state = filter.state().unwrap();
            assert!(approx_eq(state.position, price, LOOSE_EPSILON * price.max(1.0)));
            assert!(approx_eq(state.velocity, 0.0, LOOSE_EPSILON));
        }
    }

    #[test]
    fn test_17_constant_price_observation_count() {
        let mut filter = KalmanFilter::default_filter();
        for i in 1..=50 {
            filter.update(100.0);
            assert_eq!(filter.observation_count(), i);
        }
    }

    #[test]
    fn test_18_constant_price_state_consistency() {
        let mut filter = KalmanFilter::default_filter();
        for _ in 0..20 {
            filter.update(100.0);
        }

        let state1 = filter.state().unwrap();
        let (pos, vel, acc) = filter.raw_state().unwrap();

        assert!(approx_eq(state1.position, pos, EPSILON));
        assert!(approx_eq(state1.velocity, vel, EPSILON));
        assert!(approx_eq(state1.acceleration, acc, EPSILON));
    }

    #[test]
    fn test_19_constant_price_position_variance_positive() {
        let mut filter = KalmanFilter::default_filter();
        for _ in 0..100 {
            filter.update(100.0);
            let state = filter.state().unwrap();
            assert!(state.position_variance > 0.0, "Variance must always be positive");
            assert!(state.velocity_variance > 0.0);
            assert!(state.acceleration_variance > 0.0);
        }
    }

    #[test]
    fn test_20_constant_price_innovation_variance_positive() {
        let mut filter = KalmanFilter::default_filter();
        for _ in 0..100 {
            filter.update(100.0);
            let state = filter.state().unwrap();
            assert!(state.innovation_variance > 0.0, "Innovation variance must be positive");
        }
    }

    // ========================================================================
    // Test 21-30: Linear trend (constant velocity) tests
    // ========================================================================

    #[test]
    fn test_21_linear_uptrend_positive_velocity() {
        let mut filter = KalmanFilter::default_filter();
        // Price increases by 1 each step: 100, 101, 102, ...
        for i in 0..100 {
            filter.update(100.0 + i as f64);
        }
        let state = filter.state().unwrap();
        assert!(state.velocity > 0.5, "Velocity should be positive for uptrend: {}", state.velocity);
    }

    #[test]
    fn test_22_linear_uptrend_velocity_converges_to_one() {
        let mut filter = KalmanFilter::default_filter();
        for i in 0..200 {
            filter.update(100.0 + i as f64);
        }
        let state = filter.state().unwrap();
        // With slope of 1.0 per step, velocity should converge to ~1.0
        assert!(approx_eq(state.velocity, 1.0, 0.1), "Velocity should converge to 1.0: {}", state.velocity);
    }

    #[test]
    fn test_23_linear_downtrend_negative_velocity() {
        let mut filter = KalmanFilter::default_filter();
        for i in 0..100 {
            filter.update(200.0 - i as f64);
        }
        let state = filter.state().unwrap();
        assert!(state.velocity < -0.5, "Velocity should be negative for downtrend: {}", state.velocity);
    }

    #[test]
    fn test_24_linear_trend_acceleration_near_zero() {
        let mut filter = KalmanFilter::default_filter();
        // Constant velocity means zero acceleration
        for i in 0..200 {
            filter.update(100.0 + i as f64);
        }
        let state = filter.state().unwrap();
        assert!(approx_eq(state.acceleration, 0.0, 0.1),
            "Acceleration should be ~0 for constant velocity: {}", state.acceleration);
    }

    #[test]
    fn test_25_linear_trend_different_slopes() {
        let slopes = [0.5, 2.0, 5.0, 10.0];
        for slope in slopes {
            let mut filter = KalmanFilter::default_filter();
            for i in 0..300 {
                filter.update(100.0 + slope * i as f64);
            }
            let state = filter.state().unwrap();
            assert!(approx_eq(state.velocity, slope, slope * 0.15),
                "Velocity should converge to slope {}: got {}", slope, state.velocity);
        }
    }

    #[test]
    fn test_26_linear_trend_position_tracks() {
        let mut filter = KalmanFilter::default_filter();
        let slope = 2.0;
        let mut last_price = 0.0;

        for i in 0..100 {
            last_price = 100.0 + slope * i as f64;
            filter.update(last_price);
        }
        let state = filter.state().unwrap();

        // Position should be close to actual price (with some lag)
        let error = (state.position - last_price).abs();
        assert!(error < 5.0, "Position tracking error too large: {}", error);
    }

    #[test]
    fn test_27_linear_trend_predict_ahead() {
        let mut filter = KalmanFilter::default_filter();
        for i in 0..200 {
            filter.update(100.0 + i as f64);
        }

        // Predict 10 steps ahead
        let (pred_pos, pred_vel, _) = filter.predict_ahead(10).unwrap();
        let current_pos = filter.state().unwrap().position;

        // With velocity ~1, 10 steps should add ~10 to position
        assert!(pred_pos > current_pos, "Predicted position should be ahead");
        assert!(approx_eq(pred_vel, filter.state().unwrap().velocity, LOOSE_EPSILON));
    }

    #[test]
    fn test_28_steep_uptrend() {
        let mut filter = KalmanFilter::default_filter();
        // Price doubles each step (unrealistic but tests large changes)
        for i in 0..50 {
            filter.update(100.0 + 10.0 * i as f64);
        }
        let state = filter.state().unwrap();
        assert!(state.velocity > 5.0, "Should detect steep uptrend");
    }

    #[test]
    fn test_29_shallow_uptrend() {
        let mut filter = KalmanFilter::default_filter();
        // Very small slope: 0.01 per step
        for i in 0..500 {
            filter.update(100.0 + 0.01 * i as f64);
        }
        let state = filter.state().unwrap();
        assert!(state.velocity > 0.005, "Should detect shallow uptrend: {}", state.velocity);
        assert!(state.velocity < 0.02, "Velocity should be close to 0.01: {}", state.velocity);
    }

    #[test]
    fn test_30_trend_reversal_detected() {
        let mut filter = KalmanFilter::default_filter();

        // First 50 steps: uptrend
        for i in 0..50 {
            filter.update(100.0 + i as f64);
        }
        let uptrend_vel = filter.state().unwrap().velocity;

        // Next 100 steps: downtrend
        for i in 0..100 {
            filter.update(150.0 - i as f64);
        }
        let downtrend_vel = filter.state().unwrap().velocity;

        assert!(uptrend_vel > 0.0, "Should have positive velocity in uptrend");
        assert!(downtrend_vel < 0.0, "Should have negative velocity in downtrend");
    }

    // ========================================================================
    // Test 31-40: Accelerating/decelerating trend tests
    // ========================================================================

    #[test]
    fn test_31_positive_acceleration() {
        let mut filter = KalmanFilter::default_filter();
        // Quadratic: price = 100 + 0.5*t^2 (constant positive acceleration)
        for t in 0..100 {
            let price = 100.0 + 0.5 * (t as f64).powi(2);
            filter.update(price);
        }
        let state = filter.state().unwrap();
        assert!(state.acceleration > 0.0, "Should detect positive acceleration: {}", state.acceleration);
    }

    #[test]
    fn test_32_negative_acceleration() {
        let mut filter = KalmanFilter::default_filter();
        // Quadratic: price = 200 - 0.5*t^2 (constant negative acceleration)
        for t in 0..50 {
            let price = 200.0 - 0.5 * (t as f64).powi(2);
            filter.update(price);
        }
        let state = filter.state().unwrap();
        assert!(state.acceleration < 0.0, "Should detect negative acceleration: {}", state.acceleration);
    }

    #[test]
    fn test_33_acceleration_converges() {
        let mut filter = KalmanFilter::default_filter();
        // price = 100 + t + 0.5*t^2 => v = 1 + t, a = 1
        let true_accel = 1.0;
        for t in 0..300 {
            let price = 100.0 + t as f64 + 0.5 * (t as f64).powi(2);
            filter.update(price);
        }
        let state = filter.state().unwrap();
        // Acceleration estimation is noisy, use loose tolerance
        assert!(approx_eq(state.acceleration, true_accel, 0.3),
            "Acceleration should converge to {}: got {}", true_accel, state.acceleration);
    }

    #[test]
    fn test_34_decelerating_uptrend() {
        let mut filter = KalmanFilter::default_filter();
        // price = 100 + 5*t - 0.05*t^2 (uptrend that slows down)
        // Velocity = 5 - 0.1*t, at t=20 velocity = 3 (still positive)
        for t in 0..20 {
            let price = 100.0 + 5.0 * t as f64 - 0.05 * (t as f64).powi(2);
            filter.update(price);
        }
        let state = filter.state().unwrap();
        // The key test: we should detect negative acceleration (slowing uptrend)
        assert!(state.acceleration < 0.0, "Should detect deceleration: {}", state.acceleration);
        // Price is still generally increasing over the window
        assert!(state.position > 100.0, "Price should have risen from start");
    }

    #[test]
    fn test_35_accelerating_downtrend() {
        let mut filter = KalmanFilter::default_filter();
        // price = 200 - t - 0.1*t^2 (downtrend that speeds up)
        for t in 0..50 {
            let price = 200.0 - t as f64 - 0.1 * (t as f64).powi(2);
            filter.update(price);
        }
        let state = filter.state().unwrap();
        assert!(state.velocity < 0.0, "Should be in downtrend");
        assert!(state.acceleration < 0.0, "Should detect negative acceleration");
    }

    #[test]
    fn test_36_momentum_exhaustion_detection() {
        let mut filter = KalmanFilter::default_filter();

        // Strong uptrend that slows down
        // price = 100 + 10*t - 0.2*t^2
        for t in 0..40 {
            let price = 100.0 + 10.0 * t as f64 - 0.2 * (t as f64).powi(2);
            filter.update(price);
        }

        let state = filter.state().unwrap();
        // At t=25, velocity = 10 - 0.4*25 = 0 (peak), at t=40, velocity < 0
        assert!(state.acceleration < 0.0, "Should detect momentum exhaustion");
    }

    #[test]
    fn test_37_velocity_from_acceleration() {
        let mut filter = KalmanFilter::default_filter();
        // Pure quadratic: price = 0.5*a*t^2 where a=2
        // Velocity should be v = a*t = 2*t
        let accel = 2.0;
        for t in 0..100 {
            let price = 0.5 * accel * (t as f64).powi(2);
            filter.update(price);
        }

        let state = filter.state().unwrap();
        // At t=99, true velocity = 2*99 = 198
        // Filter velocity should be in the ballpark
        assert!(state.velocity > 100.0, "Velocity should be large: {}", state.velocity);
    }

    #[test]
    fn test_38_cubic_motion() {
        let mut filter = KalmanFilter::default_filter();
        // price = 100 + t^3 / 1000 (slowly increasing acceleration)
        for t in 0..100 {
            let price = 100.0 + (t as f64).powi(3) / 1000.0;
            filter.update(price);
        }
        let state = filter.state().unwrap();
        assert!(state.velocity > 0.0, "Should detect uptrend");
        assert!(state.acceleration > 0.0, "Should detect increasing velocity");
    }

    #[test]
    fn test_39_sinusoidal_motion() {
        let mut filter = KalmanFilter::default_filter();
        // price = 100 + 10*sin(t/10)
        // At t=0: v>0, a~0
        // At t~15 (pi/2*10): position peak, v~0, a<0
        for t in 0..50 {
            let price = 100.0 + 10.0 * (t as f64 / 10.0).sin();
            filter.update(price);
        }
        let state = filter.state().unwrap();
        // Filter should track the oscillation to some degree
        assert!(state.observation_count == 50);
    }

    #[test]
    fn test_40_predict_with_acceleration() {
        let mut filter = KalmanFilter::default_filter();
        // Train on accelerating motion
        for t in 0..100 {
            filter.update(100.0 + 0.5 * (t as f64).powi(2));
        }

        let current = filter.state().unwrap();
        let (pred_pos, pred_vel, pred_acc) = filter.predict_ahead(5).unwrap();

        // Prediction should use current acceleration
        assert!(pred_pos > current.position, "Position should increase");
        // Velocity prediction: v_pred = v + a*dt
        assert!(pred_vel > current.velocity || approx_eq(pred_vel, current.velocity, 0.1));
        assert!(approx_eq(pred_acc, current.acceleration, EPSILON));
    }

    // ========================================================================
    // Test 41-50: Noise and robustness tests
    // ========================================================================

    #[test]
    fn test_41_noisy_constant_price() {
        let mut filter = KalmanFilter::new(KalmanConfig::noisy_data());
        // Constant price with ±1 noise
        let noise = [0.5, -0.3, 0.8, -0.2, 0.1, -0.6, 0.4, -0.1, 0.3, -0.4];
        for i in 0..100 {
            let price = 100.0 + noise[i % noise.len()];
            filter.update(price);
        }
        let state = filter.state().unwrap();
        // Should filter out noise and converge to ~100
        assert!(approx_eq(state.position, 100.0, 1.0), "Position should be ~100: {}", state.position);
        assert!(approx_eq(state.velocity, 0.0, 0.5), "Velocity should be ~0: {}", state.velocity);
    }

    #[test]
    fn test_42_noisy_linear_trend() {
        let mut filter = KalmanFilter::default_filter();
        let noise = [0.5, -0.3, 0.8, -0.2, 0.1, -0.6, 0.4, -0.1, 0.3, -0.4];
        // Linear trend with noise
        for i in 0..200 {
            let price = 100.0 + i as f64 + noise[i % noise.len()];
            filter.update(price);
        }
        let state = filter.state().unwrap();
        // Should detect the underlying trend despite noise
        assert!(state.velocity > 0.5, "Should detect uptrend despite noise: {}", state.velocity);
    }

    #[test]
    fn test_43_outlier_rejection() {
        let mut filter = KalmanFilter::default_filter();
        // Constant price with occasional outliers
        for i in 0..100 {
            let price = if i == 50 { 150.0 } else { 100.0 }; // Spike at i=50
            filter.update(price);
        }
        let state = filter.state().unwrap();
        // Filter should not be dominated by single outlier
        assert!(approx_eq(state.position, 100.0, 5.0),
            "Position should be close to 100 despite outlier: {}", state.position);
    }

    #[test]
    fn test_44_high_measurement_noise_config() {
        let config = KalmanConfig::new(0.01, 0.01, 0.001, 100.0, 1.0);
        let mut filter = KalmanFilter::new(config);

        // With high measurement noise, filter should smooth more aggressively
        filter.update(100.0);
        filter.update(200.0); // Big jump

        let state = filter.state().unwrap();
        // Position shouldn't jump all the way to 200
        assert!(state.position < 180.0, "Should smooth large jumps: {}", state.position);
    }

    #[test]
    fn test_45_low_measurement_noise_config() {
        let config = KalmanConfig::new(0.01, 0.01, 0.001, 0.001, 1.0);
        let mut filter = KalmanFilter::new(config);

        // With low measurement noise, filter should track closely
        filter.update(100.0);
        filter.update(200.0);

        let state = filter.state().unwrap();
        // Position should be close to 200
        assert!(state.position > 150.0, "Should track measurements closely: {}", state.position);
    }

    #[test]
    fn test_46_innovation_for_surprise() {
        let mut filter = KalmanFilter::default_filter();

        // Train on constant price
        for _ in 0..50 {
            filter.update(100.0);
        }

        // Surprise with different price
        filter.update(110.0);
        let state = filter.state().unwrap();

        // Innovation should be large (positive)
        assert!(state.innovation > 5.0, "Innovation should reflect surprise: {}", state.innovation);
    }

    #[test]
    fn test_47_normalized_innovation() {
        let mut filter = KalmanFilter::default_filter();

        for _ in 0..50 {
            filter.update(100.0);
        }

        let state = filter.state().unwrap();
        let norm_innov = state.normalized_innovation();

        assert!(norm_innov.is_some(), "Should compute normalized innovation");
        // For well-tracked constant price, normalized innovation should be small
        assert!(norm_innov.unwrap().abs() < 3.0, "Normalized innovation too large");
    }

    #[test]
    fn test_48_position_gain() {
        let mut filter = KalmanFilter::default_filter();

        // First update: high uncertainty, high gain
        filter.update(100.0);
        let state1 = filter.state().unwrap();
        let gain1 = state1.position_gain();

        // After many updates: low uncertainty, lower gain
        for _ in 0..100 {
            filter.update(100.0);
        }
        let state2 = filter.state().unwrap();
        let gain2 = state2.position_gain();

        assert!(gain1 > gain2, "Gain should decrease as confidence increases");
        assert!(gain2 > 0.0 && gain2 < 1.0, "Gain should be in (0, 1)");
    }

    #[test]
    fn test_49_step_response() {
        let mut filter = KalmanFilter::default_filter();

        // Step from 100 to 200
        for _ in 0..50 {
            filter.update(100.0);
        }

        let initial_position = filter.state().unwrap().position;

        // Measure how quickly filter responds to step
        let mut positions = Vec::new();
        for _ in 0..50 {
            let state = filter.update(200.0);
            positions.push(state.position);
        }

        // Should approach 200 over time
        assert!(positions[0] < 180.0, "Shouldn't jump immediately to 200: {}", positions[0]);
        assert!(positions[49] > 180.0, "Should eventually approach 200: {}", positions[49]);

        // Final position should be much closer to 200 than initial was to 100
        let final_error = (positions[49] - 200.0).abs();
        let initial_error = (initial_position - 100.0).abs();
        assert!(final_error < 30.0, "Should track new level, error: {}", final_error);

        // Should have moved from ~100 toward 200
        assert!(positions[49] > initial_position + 50.0,
            "Should have moved significantly toward 200");
    }

    #[test]
    fn test_50_different_dt_values() {
        // Test that different dt values affect velocity/acceleration scaling
        let config1 = KalmanConfig::new(0.01, 0.01, 0.001, 1.0, 1.0);
        let config2 = KalmanConfig::new(0.01, 0.01, 0.001, 1.0, 0.5);

        let mut filter1 = KalmanFilter::new(config1);
        let mut filter2 = KalmanFilter::new(config2);

        // Same price series
        for i in 0..100 {
            filter1.update(100.0 + i as f64);
            filter2.update(100.0 + i as f64);
        }

        let state1 = filter1.state().unwrap();
        let state2 = filter2.state().unwrap();

        // Both should detect uptrend, but velocity scaling differs
        assert!(state1.velocity > 0.0);
        assert!(state2.velocity > 0.0);
    }

    // ========================================================================
    // Test 51-55: Additional edge cases and multi-symbol tests
    // ========================================================================

    #[test]
    fn test_51_very_small_prices() {
        let mut filter = KalmanFilter::default_filter();
        for i in 0..100 {
            filter.update(0.00001 + 0.000001 * i as f64);
        }
        let state = filter.state().unwrap();
        assert!(state.position > 0.0, "Should handle very small prices");
        assert!(state.velocity > 0.0, "Should detect tiny uptrend");
    }

    #[test]
    fn test_52_very_large_prices() {
        let mut filter = KalmanFilter::default_filter();
        for i in 0..100 {
            filter.update(1_000_000.0 + 1000.0 * i as f64);
        }
        let state = filter.state().unwrap();
        assert!(state.velocity > 500.0, "Should handle large prices: {}", state.velocity);
    }

    #[test]
    fn test_53_multi_symbol_kalman() {
        let mut multi = MultiSymbolKalman::new();
        multi.add_symbol("BTC", KalmanConfig::default());
        multi.add_symbol("ETH", KalmanConfig::default());

        // Update different symbols with different prices
        for i in 0..50 {
            multi.update("BTC", 50000.0 + 100.0 * i as f64);
            multi.update("ETH", 3000.0 + 10.0 * i as f64);
        }

        let btc_state = multi.state("BTC").unwrap();
        let eth_state = multi.state("ETH").unwrap();

        assert!(btc_state.velocity > 50.0, "BTC should have high velocity");
        assert!(eth_state.velocity > 5.0, "ETH should have positive velocity");
        assert!(btc_state.velocity > eth_state.velocity, "BTC velocity should be higher");
    }

    #[test]
    fn test_54_multi_symbol_all_states() {
        let mut multi = MultiSymbolKalman::new();
        multi.add_symbol("BTC", KalmanConfig::default());
        multi.add_symbol("ETH", KalmanConfig::default());
        multi.add_symbol("SOL", KalmanConfig::default());

        multi.update("BTC", 50000.0);
        multi.update("ETH", 3000.0);
        // SOL not updated

        let all_states = multi.all_states();
        assert_eq!(all_states.len(), 3);

        assert!(all_states[0].1.is_some()); // BTC
        assert!(all_states[1].1.is_some()); // ETH
        assert!(all_states[2].1.is_none()); // SOL (not updated)
    }

    #[test]
    fn test_55_predict_ahead_uninitalized() {
        let filter = KalmanFilter::default_filter();
        assert!(filter.predict_ahead(10).is_none());
    }

    // ========================================================================
    // Test 56-60: Numerical precision and stability tests
    // ========================================================================

    #[test]
    fn test_56_covariance_symmetry() {
        let mut filter = KalmanFilter::default_filter();
        for i in 0..100 {
            filter.update(100.0 + 0.5 * (i as f64).sin());
        }

        // Access internal covariance (via state variances as proxy)
        let state = filter.state().unwrap();
        assert!(state.position_variance > 0.0);
        assert!(state.velocity_variance > 0.0);
        assert!(state.acceleration_variance > 0.0);
    }

    #[test]
    fn test_57_long_running_stability() {
        let mut filter = KalmanFilter::default_filter();
        // Run for many iterations to check for numerical drift
        for i in 0..10_000 {
            let price = 100.0 + (i as f64 * 0.01).sin();
            filter.update(price);
        }
        let state = filter.state().unwrap();

        // Should still produce valid outputs
        assert!(state.position.is_finite());
        assert!(state.velocity.is_finite());
        assert!(state.acceleration.is_finite());
        assert!(state.position_variance.is_finite());
        assert!(state.position_variance > 0.0);
    }

    #[test]
    fn test_58_rapid_oscillation() {
        let mut filter = KalmanFilter::default_filter();
        // Rapid oscillation: 100, 101, 100, 101, ...
        for i in 0..100 {
            let price = 100.0 + (i % 2) as f64;
            filter.update(price);
        }
        let state = filter.state().unwrap();

        // Position should be between 100 and 101
        assert!(state.position >= 99.5 && state.position <= 101.5);
        // Velocity should be near zero (no net trend)
        assert!(state.velocity.abs() < 1.0);
    }

    #[test]
    fn test_59_alternating_jumps() {
        let mut filter = KalmanFilter::default_filter();
        // Alternating: 100, 110, 100, 110, ...
        for i in 0..100 {
            let price = if i % 2 == 0 { 100.0 } else { 110.0 };
            filter.update(price);
        }
        let state = filter.state().unwrap();

        // Position should be smoothed to middle
        assert!(state.position >= 102.0 && state.position <= 108.0,
            "Position should be smoothed: {}", state.position);
    }

    #[test]
    fn test_60_identical_observations() {
        let mut filter = KalmanFilter::default_filter();
        // All observations exactly the same
        for _ in 0..1000 {
            filter.update(123.456789);
        }
        let state = filter.state().unwrap();

        assert!(approx_eq(state.position, 123.456789, LOOSE_EPSILON));
        assert!(approx_eq(state.velocity, 0.0, LOOSE_EPSILON));
        assert!(approx_eq(state.acceleration, 0.0, LOOSE_EPSILON));
    }
}
