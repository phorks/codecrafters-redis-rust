use tokio::io::AsyncReadExt;

pub async fn skip_sequence<T: AsyncReadExt + Unpin>(buf: &mut T, str: &[u8]) -> anyhow::Result<()> {
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
