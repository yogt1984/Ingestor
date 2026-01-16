//! Research Status Screen (T-2.11)
//!
//! TUI screen for research status command (info only - display screen).

use ratatui::{
    layout::Rect,
    style::{Color, Style},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

#[derive(Debug, Clone)]
pub struct ResearchStatusScreen {
    // Info-only screen - can be enhanced with actual data display
}

impl Default for ResearchStatusScreen {
    fn default() -> Self {
        Self::new()
    }
}

impl ResearchStatusScreen {
    pub fn new() -> Self {
        Self {}
    }
}

pub fn draw_research_status_screen(
    f: &mut Frame,
    screen: &ResearchStatusScreen,
) {
    let area = f.area();
    
    let block = Block::default()
        .borders(Borders::ALL)
        .title("Research Status (Info Only)");
    
    let paragraph = Paragraph::new("This is an info-only display screen. Implementation can be enhanced to show actual research status data.")
        .style(Style::default().fg(Color::White))
        .block(block);
    
    f.render_widget(paragraph, area);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_screen() {
        let screen = ResearchStatusScreen::new();
        // Info-only screen - basic test
        assert!(true);
    }
}
