use std::{pin::Pin, task::Poll};

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt};

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
}

impl<R: AsyncBufRead + Unpin> CountingBufReader<R> {
    pub fn new(inner: R) -> Self {
        CountingBufReader {
            inner,
            total_bytes: 0,
        }
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn reset_total_bytes(&mut self) -> usize {
        let prev = self.total_bytes;
        self.total_bytes = 0;
        prev
    }

    pub fn add_total_bytes(&mut self, value: usize) {
        self.total_bytes += value;
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
            this.total_bytes += after - before;
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
        Pin::new(&mut self.get_mut().inner).consume(amt)
    }
}
