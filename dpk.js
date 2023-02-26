const crypto = require("crypto");

/*

// Existing code

exports.deterministicPartitionKey = (event) => {
  const TRIVIAL_PARTITION_KEY = "0";
  const MAX_PARTITION_KEY_LENGTH = 256;
  let candidate;

  if (event) {
    if (event.partitionKey) {
      candidate = event.partitionKey;
    } else {
      const data = JSON.stringify(event);
      candidate = crypto.createHash("sha3-512").update(data).digest("hex");
    }
  }

  if (candidate) {
    if (typeof candidate !== "string") {
      candidate = JSON.stringify(candidate);
    }
  } else {
    candidate = TRIVIAL_PARTITION_KEY;
  }
  if (candidate.length > MAX_PARTITION_KEY_LENGTH) {
    candidate = crypto.createHash("sha3-512").update(candidate).digest("hex");
  }
  return candidate;
};


*/

const TRIVIAL_PARTITION_KEY = "0";
const MAX_PARTITION_KEY_LENGTH = 256;


/*
	generates a partition key
	for the given event
*/
function get_candidate(event) {
	if (!event) return ''; //empty string, in case of no or null object
	if (event.partitionKey) {
		return event.partitionKey;
	}
	const data = JSON.stringify(event);
	return crypto.createHash("sha3-512").update(data).digest("hex");
}

/*
	serialises or encodes the
	input key to string, so that it
	can be easily transmitted or saved
	into files/disk or over networks
*/
function stringify_cand(candidate) {
	if (!candidate) return TRIVIAL_PARTITION_KEY;
	if (typeof candidate !== "string") return JSON.stringify(candidate);
	return candidate;
}


/*
	checks for max partition key length
	and performs the truncate option if needed.
*/
function truncate(candidate) {
	if (candidate.length > MAX_PARTITION_KEY_LENGTH) {
		candidate = crypto.createHash("sha3-512").update(candidate).digest("hex");
	}
	return candidate;
}


/*
	orchaestrator function
	for producing the partition key
	corresponding to an event
*/
function deterministicPartitionKey(event) {
  let candidate = get_candidate(event);
  candidate = stringify_cand(candidate);
  candidate = truncate(candidate);
  return candidate;
}

module.exports.deterministicPartitionKey = deterministicPartitionKey;

