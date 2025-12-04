use super::*;
use crate::common::{committee, keys, vote};
use crate::messages::{Vote, QC};
use types::{Digest, Hash as _, Signature};

#[test]
fn add_vote() {
    let mut aggregator = Aggregator::new(committee());
    let result = aggregator.add_vote(vote());
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn make_qc() {
    let mut aggregator = Aggregator::new(committee());
    let mut keys = keys();
    // Use a transaction hash (not QC digest) for the votes
    let tx_hash = Digest::default();

    // Create a temporary QC to compute what the QC digest will be
    let temp_qc = QC {
        hash: tx_hash.clone(),
        votes: Vec::new(),
    };
    let qc_digest = temp_qc.digest();

    // Add 2f+1 votes to the aggregator and ensure it returns the cryptographic
    // material to make a valid QC.
    // Votes should sign the QC digest, not the vote digest
    let (public_key, secret_key) = keys.pop().unwrap();
    let mut vote = Vote {
        author: public_key,
        signature: Signature::default(),
        payload: vec![tx_hash.clone()],
    };
    vote.signature = Signature::new(&qc_digest, &secret_key);
    let result = aggregator.add_vote(vote);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let (public_key, secret_key) = keys.pop().unwrap();
    let mut vote = Vote {
        author: public_key,
        signature: Signature::default(),
        payload: vec![tx_hash.clone()],
    };
    vote.signature = Signature::new(&qc_digest, &secret_key);
    let result = aggregator.add_vote(vote);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let (public_key, secret_key) = keys.pop().unwrap();
    let mut vote = Vote {
        author: public_key,
        signature: Signature::default(),
        payload: vec![tx_hash.clone()],
    };
    vote.signature = Signature::new(&qc_digest, &secret_key);
    match aggregator.add_vote(vote) {
        Ok(Some(qc)) => assert!(qc.verify(&committee()).is_ok()),
        _ => assert!(false),
    }
}

#[test]
fn cleanup() {
    let mut aggregator = Aggregator::new(committee());

    // Add a vote and ensure it is in the aggregator memory.
    let result = aggregator.add_vote(vote());
    assert!(result.is_ok());
    assert_eq!(aggregator.votes_aggregators.len(), 1);

    // Clean up the aggregator.
    aggregator.cleanup(&Digest::default());
    assert!(aggregator.votes_aggregators.is_empty());
}
