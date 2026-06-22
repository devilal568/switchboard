use crate::error::SwitchBoardError;

pub trait Publication: Sized {
    type Config;
    fn new(config: &Self::Config) -> Result<Self, SwitchBoardError>;
    fn cleanup(&mut self);
}
