use std::{pin::Pin, task::Poll};

use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader},
    net::tcp::OwnedReadHalf,
};

pub type Reader = CountingBufReader<BufReader<OwnedReadHalf>>;

pub async fn skip_sequence<W: AsyncReadExt + Unpin>(buf: &mut W, str: &[u8]) -> anyhow::Result<()> {
    for i in 0..str.len() {
        let ch = buf.read_u8().await?;
        if ch != str[i] {
            anyhow::bail!(
                "Expected character: {}, received: {}",
                str[i] as char,
                ch as char
            );
        }
    }

    Ok(())
}

pub async fn read_until_crlf<R: AsyncBufReadExt + Unpin>(buf: &mut R) -> anyhow::Result<Vec<u8>> {
    let mut res = vec![];
    loop {
        let ch = buf.read_u8().await?;
        if ch == b'\r' {
            let nch = buf.read_u8().await?;
            if nch == b'\n' {
                return Ok(res);
            }

            res.push(ch);
            res.push(nch);
        } else {
            res.push(ch);
        }
    }
}

pub async fn skip_crlf<R: AsyncBufReadExt + Unpin>(buf: &mut R) -> anyhow::Result<()> {
    skip_sequence(buf, &[b'\r', b'\n']).await
}

pub struct CountingBufReader<R: AsyncBufRead + Unpin> {
    inner: R,
    total_bytes: usize,
    counting_scopes: Vec<usize>,
}

impl<R: AsyncBufRead + Unpin> CountingBufReader<R> {
    pub fn new(inner: R) -> Self {
        CountingBufReader {
            inner,
            total_bytes: 0,
            counting_scopes: vec![0],
        }
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn new_counting_scope(&mut self) -> ScopedCountingGuard<'_, R> {
        self.counting_scopes.push(0);
        ScopedCountingGuard { reader: self }
    }

    pub fn reset_counting(&mut self) -> anyhow::Result<()> {
        self.total_bytes = 0;

        if self.counting_scopes.len() > 1 {
            anyhow::bail!("Attempt to reset counting while there are active scopes");
        }

        self.counting_scopes = vec![0];
        Ok(())
    }

    // pub fn end_counting_scope(&mut self) ->  {
    //     let n = self.current_scope_bytes;
    //     self.current_scope_bytes += self
    //         .scope_bytes_stack
    //         .pop()
    //         .ok_or(anyhow::anyhow!("Stack is empty"))?;
    //     n
    // }

    // pub fn reset_total_bytes(&mut self) -> usize {
    //     let prev = self.total_bytes;
    //     self.total_bytes = 0;
    //     prev
    // }
    //
    // pub fn add_total_bytes(&mut self, value: usize) {
    //     self.total_bytes += value;
    // }
}

pub struct ScopedCountingGuard<'a, R: AsyncBufRead + Unpin> {
    reader: &'a mut CountingBufReader<R>,
}

impl<'a, R: AsyncBufRead + Unpin> ScopedCountingGuard<'a, R> {
    pub fn count_so_far(&self) -> usize {
        self.reader.counting_scopes.last().unwrap().clone()
    }

    pub fn reader(&mut self) -> &mut CountingBufReader<R> {
        &mut self.reader
    }
}

impl<'a, R: AsyncBufRead + Unpin> Drop for ScopedCountingGuard<'a, R> {
    fn drop(&mut self) {
        let stack = &mut self.reader.counting_scopes;
        let scoped_count = stack.pop().unwrap();
        *stack.last_mut().unwrap() += scoped_count;
    }
}

impl<R: AsyncBufRead + Unpin> AsyncRead for CountingBufReader<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();

        let before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        let after = buf.filled().len();

        if matches!(result, Poll::Ready(Ok(()))) {
            let n_bytes = after - before;
            this.total_bytes += n_bytes;
            *this.counting_scopes.last_mut().unwrap() += n_bytes;
        }

        result
    }
}

impl<R: AsyncBufRead + Unpin> AsyncBufRead for CountingBufReader<R> {
    fn poll_fill_buf(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<&[u8]>> {
        Pin::new(&mut self.get_mut().inner).poll_fill_buf(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.total_bytes += amt;
        *self.counting_scopes.last_mut().unwrap() += amt;
        Pin::new(&mut self.get_mut().inner).consume(amt)
    }
}
