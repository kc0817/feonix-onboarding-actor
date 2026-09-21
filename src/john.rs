use tokio::sync::{mpsc, oneshot};

use crate::*;

// ##################################################### //
// ################### ACTOR BACKEND ################### //
// ##################################################### //

/// This is our Actor John (which just happens to be the name of PART's VIP Coordinator 🤯🤯🤯)
struct John {
	// Actor John receives messages via `receiver`
	//  - Note: mpsc stands for multiple-producer-single-consumer, multiple `Sender<>` can exist for one `Receiver<>`
	receiver: mpsc::Receiver<JohnMessage>,

	underlings: Vec<String>, 	// Vector (list) of VIP student names
	underling_grades: Vec<f64>, // Vector (list) of VIP student grades
	booster_handle: Option<BoosterHandle>, // Booster Actor's handle
}

/// This enum of messages cover all functionality that we might possibly want from our Actor.
///  - Note: Rust enums can hold values, sort of like mini structs.
#[derive(Debug)]
enum JohnMessage {
	AddUnderling { name: String },
	SetUnderlingGrade { name: String, grade: f64 },
	SetBooster { booster_handle: BoosterHandle },
	SendAllToBooster { reply_to: oneshot::Sender<()>  }, // IMPORTANT: `reply_to` IS USED TO CONFIRM WHEN OPERATION IS DONE
}

/// Define methods for our Actor John
///  - Note: notice how `John` methods are NOT public (no `pub`), only `JohnHandle` methods are public (has `pub`)
impl John {
	fn new(receiver: mpsc::Receiver<JohnMessage>) -> Self {
		John {
			receiver: receiver,
			booster_handle: None,
			underlings: Vec::new(),
			underling_grades: Vec::new(),
		}
	}

	async fn handle_message(&mut self, msg: JohnMessage) {
		println!("[ACTOR]: John is running handle_message() with new JohnMessage: {:?}", msg);

		match msg {

			JohnMessage::AddUnderling { name } => {
				println!("[ACTOR]: John adding a new underling {}", name);

				self.underlings.push(name);
				self.underling_grades.push(0.0);
			},

			JohnMessage::SetUnderlingGrade { name, grade } => {
				println!("[ACTOR]: John setting {} grade to {}", name, grade);

				let found_index: Option<usize> = self.underlings.iter().position(|n| *n == name);
				if let Some(ind) = found_index {
					self.underling_grades[ind] = grade;
				}

				// Note: ^^^ this is the "rusty" way of checking and unwrapping an `Option<T>`, it's equivalent to:
				//        if found_index.is_some() {
				//             let ind = found_index.unwrap();
			},
			
			JohnMessage::SetBooster { booster_handle } => {
				println!("[ACTOR]: John initializing BoosterHandle field with BoosterHandle");

				self.booster_handle = Some(booster_handle);
							// Note: ^ since `self.booster_handle` is an `Option<T>` that can take either `Some(T)` or `None`
			},
			
			JohnMessage::SendAllToBooster { reply_to } => {
				if let Some(bh) = &self.booster_handle {
					// Note: ^ this is the "rusty" way of checking and unwrapping an `Option<T>`, it's equivalent to:
					//        if self.booster_handle.is_some() {
					//             let bs = self.booster_handle.unwrap();

					println!("[ACTOR]: John entering all students and grades to BoosterHandle");

					bh.enter_student_names(self.underlings.clone()).await;
					bh.enter_student_grades(self.underling_grades.clone()).await;
				}
				else {
					eprintln!("[ACTOR]: John does not have BoosterHandle initialized so nothing happened");
				}

				// IMPORTANT: WE NEED A CALLBACK TO SEND AN EMPTY TUPLE `()` ACROSS CHANNEL TO TELL JOHNHANDLE "EVERYTHING IS DONE"
				let _ = reply_to.send(());
			},
		}
	}
}

// Note: EVERYTHING WRITTEN ABOVE IS THE ACTOR ENCAPSULATED BEHIND A HANDLE `JohnHandle`
//  - We as programmers NEVER interact with the Actor (backend) directly, we interact with an actor via its Handle (frontend)

// ###################################################### //
// ################### ACTOR FRONTEND ################### //
// ###################################################### //


/// This is the Handle for our Actor John, it's very easily cloned and passed around.
#[derive(Clone, Debug)]
pub struct JohnHandle {
	sender: mpsc::Sender<JohnMessage>,
}

/// This ASYNC function starts up and runs the actor backend
///  - Initially, `receiver` is waiting and blocking until it receives a `JohnMessage`
///  - When a `JohnMessage` is received, it runs `handle_message()` and then goes back to waiting and blocking
async fn run_john_actor(mut actor: John) {
	println!("[run_john_actor()]: is blocking until a JohnMessage is received...");
	while let Some(msg) = actor.receiver.recv().await {
		println!("\n[run_john_actor()]: received a new JohnMessage and calling handle_message()...");
		actor.handle_message(msg).await;
	}
}

impl JohnHandle {
	/// ### IMPORTANT METHOD: ###
	/// This is the constructor, return type is `Self` which is identical to having a return type of `JohnHandle`
	///   - Call constructor with `let john_handle = JohnHandle::new();`
	pub async fn new() -> Self {
		let (sender, receiver) = mpsc::channel(8); // First, we make the communication channel sender-receiver pair
		let actor: John = John::new(receiver);	   // Next, we call the John Actor constructor from HERE ONLY, never anywhere else, and assign the receiver to it

		// Then, we start running the John Actor (backend) since it now has its `receiver`, it can start listening for messages
		//  - IMPORTANT: WE MAKE `run_john_actor()` RUN AS A SEPARATE `tokio` TASK WITH `tokio::spawn`
		tokio::spawn(run_john_actor(actor));

		// Finally, we make and return our John Handle (frontend) with its `sender`, and we can use it to send messages.
		//  - Note: we don't need an explicit `return` if it's the last line and doesn't have a closing semicolon.
		JohnHandle {
			sender: sender
		}
	}

	pub async fn register_new_student(&self, name: String) {
		let msg: JohnMessage = JohnMessage::AddUnderling { name: name, };
		let _ = self.sender.send(msg).await;
		//  ^ rust-analyzer complains when you don't use a returned result, this is jus a way of telling
		//    it that the returned result doesn't matter
	}

	pub async fn assign_grade_to_student(&self, name: String, grade: f64) {
			let msg: JohnMessage = JohnMessage::SetUnderlingGrade { name: name, grade: grade };
		let _ = self.sender.send(msg).await;
	}

	pub async fn set_booster(&self, booster_handle: BoosterHandle) {
		let msg: JohnMessage = JohnMessage::SetBooster { booster_handle };
		let _ = self.sender.send(msg).await;
	}

	pub async fn report_all_students_and_grades_to_booster(&self) {
		let (tx, rx) = oneshot::channel();

		let msg: JohnMessage = JohnMessage::SendAllToBooster { reply_to: tx };
		let _ = self.sender.send(msg).await;
		
		let _ = rx.await;
	}
}

// THOUGHT EXERCISES:
// Why is `run_john_actor()` async? Why can't this be a normal synchronous function?
// When we want to add new functionality / new methods in Actor John, what need to be updated?



