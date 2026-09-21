use tokio::sync::{mpsc, oneshot};

use crate::*;

// ##################################################### //
// ################### ACTOR BACKEND ################### //
// ##################################################### //

struct Booster {
    receiver: mpsc::Receiver<BoosterMessage>,
    underling_names: Vec<String>,
    underling_grades: Vec<f64>
}

#[derive(Debug)]
enum BoosterMessage {
    BoostGrade { name: String, amt: f64 },
    BoostAura { name: String }
}

impl Booster {
	fn new(receiver: mpsc::Receiver<BoosterMessage>) -> Self {
        Booster { 
            receiver: receiver, 
            underling_names: Vec::new(), 
            underling_grades: Vec::new() 
        }
    }

    fn find_name(name: &String) -> Option<i32> {
        let mut i = 0;
        loop {
            if i >= self.underlingNames.len() {
                return None
            }
            if let Some(name2) = self.underlingNames[i] && name2 == name {
                return Some(i)
            }

            i += 1;
        }
    }

    async fn handle_message(&mut self, msg: BoosterMessage) {
        println!("[Actor] Booster is running handle_message() with new BoosterMessage: {:?}", msg);
        match msg {
            BoosterMessage::BoostGrade {name, amt} => {
                let index = find_name(&name);
                if let Some(i) = index {
                    self.underling_grades[i] += amt;
                }
            },

            BoosterMessage::BoostAura {name} => {
                let index = find_name(&name);
                if let Some(i) = index {
                    self.underling_names[i] += " da rizzler";
                }
            }
        };
    }
}

// ###################################################### //
// ################### ACTOR FRONTEND ################### //
// ###################################################### //

async fn run_booster_actor(mut actor: Booster) {
    while let Some(msg) = actor.receiver.recv().await {
        println!("\nBooster actor received a new msg!");
        actor.handle_message(msg).await;
    }
}

#[derive(Clone, Debug)]
pub struct BoosterHandle {
    sender: mpsc::Sender<BoosterMessage>,
}

impl BoosterHandle {
    pub async fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);

        let actor: Booster = Booster::new(receiver);
        tokio::spawn(run_booster_actor(actor));
        
        BoosterHandle { sender }
    }

    pub async fn boost_grades(&self, name: String, amt: f64) {

    }
}
