//! Parameter Input Widgets
//!
//! This module contains reusable widgets for parameter configuration in the TUI.
//! These widgets are used for interactive parameter editing.

pub mod text_input;
pub mod number_input;
pub mod comma_list;
pub mod toggle;
pub mod path_input;

pub use text_input::TextInputWidget;
pub use number_input::NumberInputWidget;
pub use comma_list::CommaListWidget;
pub use toggle::ToggleWidget;
pub use path_input::PathInputWidget;

