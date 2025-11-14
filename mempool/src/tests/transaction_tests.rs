#[test]
fn transaction_data_serialize_deserialize_round_trip() {
    let mut rng = StdRng::from_seed([42; 32]);
    let (sender, _) = generate_keypair(&mut rng);
    let (destination, _) = generate_keypair(&mut rng);

    let original = TransactionData {
        sender,
        amount: 1000,
        destination,
        nonce: 5,
        epoch: 1,
    };

    // Serialize to Transaction
    let serialized = original
        .to_transaction()
        .expect("Serialization should succeed");
    assert!(
        !serialized.is_empty(),
        "Serialized transaction should not be empty"
    );

    // Deserialize back to TransactionData
    let deserialized =
        TransactionData::from_transaction(&serialized).expect("Deserialization should succeed");

    // Verify all fields match
    assert_eq!(original.sender, deserialized.sender);
    assert_eq!(original.amount, deserialized.amount);
    assert_eq!(original.destination, deserialized.destination);
    assert_eq!(original.nonce, deserialized.nonce);
    assert_eq!(original.epoch, deserialized.epoch);
    assert_eq!(original, deserialized);
}

#[test]
fn transaction_data_serialize_deserialize_with_zero_values() {
    let mut rng = StdRng::from_seed([1; 32]);
    let (sender, _) = generate_keypair(&mut rng);
    let (destination, _) = generate_keypair(&mut rng);

    let original = TransactionData {
        sender,
        amount: 0,
        destination,
        nonce: 0,
        epoch: 0,
    };

    let serialized = original
        .to_transaction()
        .expect("Serialization should succeed");
    let deserialized =
        TransactionData::from_transaction(&serialized).expect("Deserialization should succeed");

    assert_eq!(original, deserialized);
}

#[test]
fn transaction_data_serialize_deserialize_with_max_values() {
    let mut rng = StdRng::from_seed([2; 32]);
    let (sender, _) = generate_keypair(&mut rng);
    let (destination, _) = generate_keypair(&mut rng);

    let original = TransactionData {
        sender,
        amount: u64::MAX,
        destination,
        nonce: u32::MAX,
        epoch: u64::MAX,
    };

    let serialized = original
        .to_transaction()
        .expect("Serialization should succeed");
    let deserialized =
        TransactionData::from_transaction(&serialized).expect("Deserialization should succeed");

    assert_eq!(original, deserialized);
}

#[test]
fn transaction_data_serialize_deserialize_different_keys() {
    let mut rng = StdRng::from_seed([3; 32]);
    let (sender1, _) = generate_keypair(&mut rng);
    let (destination1, _) = generate_keypair(&mut rng);
    let (sender2, _) = generate_keypair(&mut rng);
    let (destination2, _) = generate_keypair(&mut rng);

    let tx1 = TransactionData {
        sender: sender1,
        amount: 100,
        destination: destination1,
        nonce: 1,
        epoch: 1,
    };

    let tx2 = TransactionData {
        sender: sender2,
        amount: 200,
        destination: destination2,
        nonce: 2,
        epoch: 2,
    };

    let serialized1 = tx1.to_transaction().expect("Serialization should succeed");
    let serialized2 = tx2.to_transaction().expect("Serialization should succeed");

    // Serialized transactions should be different
    assert_ne!(serialized1, serialized2);

    let deserialized1 =
        TransactionData::from_transaction(&serialized1).expect("Deserialization should succeed");
    let deserialized2 =
        TransactionData::from_transaction(&serialized2).expect("Deserialization should succeed");

    assert_eq!(tx1, deserialized1);
    assert_eq!(tx2, deserialized2);
    assert_ne!(deserialized1, deserialized2);
}

#[test]
fn transaction_data_deserialize_invalid_data() {
    // Invalid data that cannot be deserialized
    let invalid_data: Transaction = vec![0, 1, 2, 3, 4, 5];

    let result = TransactionData::from_transaction(&invalid_data);
    assert!(
        result.is_err(),
        "Deserialization of invalid data should fail"
    );
}

#[test]
fn transaction_data_serialize_produces_valid_transaction() {
    let mut rng = StdRng::from_seed([4; 32]);
    let (sender, _) = generate_keypair(&mut rng);
    let (destination, _) = generate_keypair(&mut rng);

    let tx_data = TransactionData {
        sender,
        amount: 500,
        destination,
        nonce: 10,
        epoch: 3,
    };

    let transaction = tx_data
        .to_transaction()
        .expect("Serialization should succeed");

    // Verify it's a valid Vec<u8>
    assert!(!transaction.is_empty());

    // Verify we can deserialize it back
    let deserialized = TransactionData::from_transaction(&transaction)
        .expect("Should be able to deserialize valid transaction");
    assert_eq!(tx_data, deserialized);
}
