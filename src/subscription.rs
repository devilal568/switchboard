use crate::error::SwitchBoardError;

pub trait Subscription: Sized {
    type Config;
    fn new(config: Self::Config) -> Result<Self, SwitchBoardError>;
    fn cleanup(&mut self);
}
