use crate::error::SwitchBoardError;

// pub trait Publisher {
//     type Data:Send;
//     fn publish(&mut self, payload: Self::Data) -> impl Future<Output = Result<(), SwitchBoardError>> + Send;
// }


pub trait Publisher {
    fn publish(&mut self, payload: &[u8]) -> Result<(), SwitchBoardError>;
}


