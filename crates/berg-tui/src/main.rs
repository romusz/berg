use std::{io, time::Duration};

use anyhow::Context;
use crossterm::{
    cursor::Show,
    event::{self, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    widgets::{Block, Borders, Paragraph},
};

type TerminalHandle = Terminal<CrosstermBackend<io::Stdout>>;

/// `RAII` guard that restores the terminal on drop: leaves the alternate
/// screen, shows the cursor, and disables raw mode.
///
/// Constructed *after* `enable_raw_mode()` succeeds, so a partial-init
/// failure (e.g., entering the alternate screen fails) still triggers
/// cleanup when the guard unwinds.
struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> anyhow::Result<Self> {
        install_panic_hook();
        enable_raw_mode().context("enable raw mode")?;
        // Guard now exists; any subsequent failure unwinds through Drop.
        let guard = Self;
        execute!(io::stdout(), EnterAlternateScreen).context("enter alternate screen")?;
        Ok(guard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_terminal();
    }
}

fn install_panic_hook() {
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        restore_terminal();
        hook(panic_info);
    }));
}

fn restore_terminal() {
    // Best-effort cleanup; errors are ignored because we may already be
    // unwinding from another error or panic. Raw mode is disabled first because
    // it has more side effects than leaving the alternate screen.
    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen, Show);
}

fn main() -> anyhow::Result<()> {
    let _guard = TerminalGuard::enter()?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend).context("create terminal")?;
    run(&mut terminal)
}

fn run(terminal: &mut TerminalHandle) -> anyhow::Result<()> {
    let message = berg_core::welcome_message("berg-tui")?;

    loop {
        terminal
            .draw(|frame| {
                let widget = Paragraph::new(format!("{message}\n\nPress q or Esc to quit."))
                    .block(Block::default().title("berg-tui").borders(Borders::ALL));

                frame.render_widget(widget, frame.area());
            })
            .context("draw frame")?;

        if event::poll(Duration::from_millis(250)).context("poll events")?
            && let Event::Key(key) = event::read().context("read event")?
            && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
        {
            break;
        }
    }

    Ok(())
}
