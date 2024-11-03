pub struct ServerConfig {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
}

impl ServerConfig {
    pub fn new(args: Vec<String>) -> ServerConfig {
        let mut it = args.iter();
        let mut dir = None;
        let mut dbfilename = None;
        while let Some(arg) = it.next() {
            if arg == "--dir" {
                if let Some(next) = it.next() {
                    dir = Some(next.clone());
                } else {
                    panic!("'dir' value expected");
                }
            } else if arg == "--dbfilename" {
                if let Some(next) = it.next() {
                    dbfilename = Some(next.clone());
                } else {
                    panic!("'dbfilename value expected");
                }
            }
        }

        ServerConfig { dir, dbfilename }
    }
}
