use crate::resp::RespMessage;

pub trait ToMapRespArray {
    fn to_map_resp_array(self) -> RespMessage;
    fn to_flattened_map_resp_array(self) -> RespMessage;
}

impl<I> ToMapRespArray for I
where
    I: IntoIterator<Item = (RespMessage, RespMessage)>,
{
    fn to_map_resp_array(self) -> RespMessage {
        let items = self.into_iter().map(|x| RespMessage::Array(vec![x.0, x.1]));
        RespMessage::Array(items.collect())
    }

    fn to_flattened_map_resp_array(self) -> RespMessage {
        let items = self.into_iter().flat_map(|x| [x.0, x.1]);
        RespMessage::Array(items.collect())
    }
}

pub trait ToStringResp {
    fn to_simple_string(&self) -> RespMessage;
    fn to_bulk_string(&self) -> RespMessage;
}

impl<S: ToString> ToStringResp for S {
    fn to_simple_string(&self) -> RespMessage {
        RespMessage::SimpleString(self.to_string())
    }

    fn to_bulk_string(&self) -> RespMessage {
        RespMessage::BulkString(self.to_string())
    }
}
