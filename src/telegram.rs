use std::env;

pub fn send_alert(msg: &str) {
    let token = match env::var("TELEGRAM_BOT_TOKEN") {
        Ok(t) => t,
        _ => return,
    };
    let chat_id = match env::var("TELEGRAM_CHAT_ID") {
        Ok(c) => c,
        _ => return,
    };
    let url = format!(
        "https://api.telegram.org/bot{}/sendMessage",
        token
    );
    let payload = serde_json::json!({
        "chat_id": chat_id,
        "text": msg,
        "parse_mode": "HTML"
    });

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let _ = client
            .post(&url)
            .json(&payload)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await;
    });
}
