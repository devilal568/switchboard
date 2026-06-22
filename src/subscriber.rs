use crate::error::SwitchBoardError;

pub trait Subscriber {
    fn listen(&mut self) -> Result<Option<Vec<u8>>, SwitchBoardError>;
}
