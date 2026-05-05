use std::{io, time::Duration};

use anyhow::Context;
use crossterm::{
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

fn main() -> anyhow::Result<()> {
    let mut terminal = init_terminal()?;
    let result = run(&mut terminal);
    let restore_result = restore_terminal(&mut terminal);

    result?;
    restore_result?;

    Ok(())
}

fn init_terminal() -> anyhow::Result<TerminalHandle> {
    enable_raw_mode().context("enable raw mode")?;

    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).context("enter alternate screen")?;

    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend).context("create terminal")
}

fn restore_terminal(terminal: &mut TerminalHandle) -> anyhow::Result<()> {
    disable_raw_mode().context("disable raw mode")?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen).context("leave alternate screen")?;
    terminal.show_cursor().context("show cursor")?;

    Ok(())
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
