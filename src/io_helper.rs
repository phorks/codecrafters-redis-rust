use tokio::io::{AsyncBufReadExt, AsyncReadExt};

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

pub async fn skip_crlf<R: AsyncReadExt + Unpin>(buf: &mut R) -> anyhow::Result<()> {
    skip_sequence(buf, &[b'\r', b'\n']).await
}
