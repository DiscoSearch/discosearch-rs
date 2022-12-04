use hyper::{Client, Uri, Request, client::connect::HttpConnector, Method, Body};

#[derive(Debug)]
pub struct DiscoClient {
    client: Client<HttpConnector>,
    base_url: String,
}

impl Default for DiscoClient {
    fn default() -> Self {
        Self { client: Client::new(), base_url: String::from("http://localhost:8000") }
    }
}

impl DiscoClient {
    pub fn new(base_url: &str) -> Self {
        Self { client: Client::new(), base_url: String::from(base_url) }
    }

    pub async fn create_index(&self, name: &str, dim: u8) -> Result<(), String> {
        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("{}/index", self.base_url))
            .header("content-type", "application/json")
            .body(Body::from(format!(r#"{{"name": "{}", "dim": {}}}"#, name, dim)))
            .unwrap();

        let result = match self.client.request(req).await {
            Ok(response) if response.status() == 200 => Ok(()),
            _ => Err(String::from("Create index request failed"))
        };

        result
    }

    pub async fn get_all_indexes(&self) -> Result<Vec<String>, String> {
        let uri = format!("{}/index", self.base_url).parse::<Uri>().unwrap();
        let result = match self.client.get(uri).await {
            Ok(response) => {
                let body = response.into_body();
                let body_bytes = hyper::body::to_bytes(body).await.unwrap();
                let result: Vec<String> = serde_json::from_str(&String::from_utf8(body_bytes.to_vec()).unwrap()).unwrap();
                result
            },
            Err(err) => { return Err(err.to_string()) }
        };

        Ok(result)
    }

    pub async fn insert_vector(&self, index_name: &str, vector: Vec<f64>) -> Result<(), String> {
        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("{}/index/{}/vectors", self.base_url, index_name))
            .header("content-type", "application/json")
            .body(Body::from(format!(r#"{{"vec": {:?}}}"#, vector)))
            .unwrap();

        let result = match self.client.request(req).await {
            Ok(response) if response.status() == 200 => Ok(()),
            _ => Err(String::from("Insert vector request failed"))
        };

        result
    }

    pub async fn query_knn(&self, index_name: &str, query_vector: Vec<f32>, k: u8) -> Result<Vec<String>, String> {
        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("{}/index/{}/query", self.base_url, index_name))
            .header("content-type", "application/json")
            .body(Body::from(format!(r#"{{"query": {:?}, "k": {}}}"#, query_vector, k)))
            .unwrap();

        let result = match self.client.request(req).await {
            Ok(response) if response.status() == 200 => {
                let body = response.into_body();
                let body_bytes = hyper::body::to_bytes(body).await.unwrap();
                let result: Vec<String> = serde_json::from_str(&String::from_utf8(body_bytes.to_vec()).unwrap()).unwrap();
                Ok(result)
            },
            _ => Err(String::from("Query request failed"))
        };

        result
    }
}
