use tokio::sync::{mpsc, oneshot};

use crate::*;

// ##################################################### //
// ################### ACTOR BACKEND ################### //
// ##################################################### //

struct Booster {
    receiver: mpsc::Receiver<BoosterMessage>,
    underling_names: Vec<String>,
    underling_grades: Vec<f64>,
    brightspace: Option<BrightspaceHandle>
}

#[derive(Debug)]
enum BoosterMessage {
    EnterNames { names: Vec<String> },
    EnterGrades { grades: Vec<f64> },
    BoostGrade { name: String, amt: f64 },
    BoostAura { name: String },
    SetBrightspace { brightspace: BrightspaceHandle },
    SendToBrightspace { reply: oneshot::Sender<()> }
}

impl Booster {
	fn new(receiver: mpsc::Receiver<BoosterMessage>) -> Self {
        Booster { 
            receiver: receiver, 
            underling_names: Vec::new(), 
            underling_grades: Vec::new(),
            brightspace: None
        }
    }

    fn find_name(&self, name: &String) -> Option<usize> {
        let mut i = 0;
        loop {
            if i >= self.underling_names.len() {
                return None
            }
            if self.underling_names[i] == *name {
                return Some(i)
            }

            i += 1;
        }
    }

    async fn handle_message(&mut self, msg: BoosterMessage) {
        println!("[Actor] Booster is running handle_message() with new BoosterMessage: {:?}", msg);
        match msg {
            BoosterMessage::BoostGrade { name, amt } => {
                let index = self.find_name(&name);
                if let Some(i) = index {
                    self.underling_grades[i] += amt;
                }
            },

            BoosterMessage::BoostAura { name } => {
                let index = self.find_name(&name);
                if let Some(i) = index {
                    self.underling_names[i] += " da rizzler";
                }
            },
            BoosterMessage::EnterNames { names } => {
                self.underling_names = names;
            },
            BoosterMessage::EnterGrades { grades } => {
                self.underling_grades = grades;
            },
            BoosterMessage::SetBrightspace { brightspace } => {
                self.brightspace = Some(brightspace);
            },
            BoosterMessage::SendToBrightspace { reply } => {
                if let Some(bs) = &self.brightspace {
                    bs.enter_students_into_brightspace(self.underling_names.clone()).await;
                    bs.enter_student_grades_into_brightspace(self.underling_grades.clone()).await;
                }
                reply.send(());
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

    pub async fn boost_grade(&self, name: String, amt: f64) {
        let data = BoosterMessage::BoostGrade { name, amt };
        self.sender.send(data).await;
    }
    pub async fn boost_aura(&self, name: String) {
        let data = BoosterMessage::BoostAura { name };
        self.sender.send(data).await;
    }

    pub async fn enter_student_names(&self, names: Vec<String>) {
        let data = BoosterMessage::EnterNames { names };
        self.sender.send(data).await;
    }
    pub async fn enter_student_grades(&self, grades: Vec<f64>) {
        let data = BoosterMessage::EnterGrades { grades };
        self.sender.send(data).await;
    }
    pub async fn set_brightspace(&self, brightspace: BrightspaceHandle) {
        let data = BoosterMessage::SetBrightspace { brightspace };
        self.sender.send(data).await;
    }
    pub async fn send_to_brightspace(&self) {
        let (tx, rx) = oneshot::channel();
        let data = BoosterMessage::SendToBrightspace { reply: tx };
        self.sender.send(data).await;
        _ = rx.await;
    }
}
