use crate::config::{Committee, Stake};
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::{Vote, QC};
use crypto::{Digest, PublicKey, Signature};
use std::collections::{HashMap, HashSet};

#[cfg(test)]
#[path = "tests/aggregator_tests.rs"]
pub mod aggregator_tests;

pub struct Aggregator {
    committee: Committee,
    votes_aggregators: HashMap<Digest, Box<QCMaker>>,
}

impl Aggregator {
    pub fn new(committee: Committee) -> Self {
        Self {
            committee,
            votes_aggregators: HashMap::new(),
        }
    }

    pub fn add_vote(&mut self, vote: Vote) -> ConsensusResult<Option<QC>> {
        // TODO [issue #7]: A bad node may make us run out of memory by sending many votes
        // with different digests.

        // For single transaction votes, payload should contain exactly one digest.
        // Extract the transaction digest from the payload.
        ensure!(vote.payload.len() == 1, ConsensusError::InvalidPayload);
        let digest = vote.payload[0].clone();

        // Add the new vote to our aggregator and see if we have a QC.
        self.votes_aggregators
            .entry(digest)
            .or_insert_with(|| Box::new(QCMaker::new()))
            .append(vote, &self.committee)
    }

    /// Add a batch vote: extract each digest from the payload and add the author's vote for each.
    /// The batch vote signature is verified separately before calling this method.
    /// This method directly adds the author and signature to each digest's quorum tracker
    /// without creating individual Vote structures.
    pub fn add_batch_vote(
        &mut self,
        batch_vote: Vote,
    ) -> ConsensusResult<Vec<(Digest, Option<QC>)>> {
        let mut results = Vec::new();
        let author = batch_vote.author;
        let signature = batch_vote.signature;

        for digest in &batch_vote.payload {
            // Directly add the author and signature to the quorum tracker for this digest.
            let qc = self
                .votes_aggregators
                .entry(digest.clone())
                .or_insert_with(|| Box::new(QCMaker::new()))
                .append_author_signature(
                    digest.clone(),
                    author,
                    signature.clone(),
                    &self.committee,
                )?;

            results.push((digest.clone(), qc));
        }

        Ok(results)
    }
}

struct QCMaker {
    weight: Stake,
    votes: Vec<(PublicKey, Signature)>,
    used: HashSet<PublicKey>,
}

impl QCMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(&mut self, vote: Vote, committee: &Committee) -> ConsensusResult<Option<QC>> {
        // For single transaction votes, payload contains exactly one digest.
        let digest = vote.payload[0].clone();
        self.append_author_signature(digest, vote.author, vote.signature, committee)
    }

    /// Append an author's signature directly to the quorum tracker.
    pub fn append_author_signature(
        &mut self,
        hash: Digest,
        author: PublicKey,
        signature: Signature,
        committee: &Committee,
    ) -> ConsensusResult<Option<QC>> {
        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuse(author)
        );

        self.votes.push((author, signature));
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures QC is only made once.
            return Ok(Some(QC {
                hash,
                votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}
