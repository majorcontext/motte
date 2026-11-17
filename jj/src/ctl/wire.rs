//! The control protocol on the wire: the typed requests, the error codes and the answers.

use serde::{Deserialize, Serialize};

/// The largest object the store takes, and the largest answer of `getbatch`.
pub const MAX_OBJECT_BYTES: u64 = 16 << 20;

/// The code of an error answer. A shim branches on it, never on the `err` text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Code {
    InvalidRequest,
    UnknownOp,
    LineTooLong,
    StoreUnavailable,
    InternalError,
    ObjectTooLarge,
    NotFound,
    GenerationConflict,
    ClaimLost,
    /// Any other wire code of the store, kept as sent.
    Other(String),
}

impl Code {
    pub fn as_str(&self) -> &str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::UnknownOp => "unknown_op",
            Self::LineTooLong => "line_too_long",
            Self::StoreUnavailable => "store_unavailable",
            Self::InternalError => "internal_error",
            Self::ObjectTooLarge => "vcs_object_too_large",
            Self::NotFound => "vcs_not_found",
            Self::GenerationConflict => "vcs_generation_conflict",
            Self::ClaimLost => "vcs_claim_lost",
            Self::Other(code) => code,
        }
    }

    /// The code of an error answer. An answer with no `code` comes from a daemon that sends none;
    /// this is the one place that reads the `err` text of such a daemon.
    pub fn of_answer(code: Option<&str>, err: &str) -> Self {
        if let Some(code) = code {
            return Self::from_wire(code);
        }
        const TEXTS: [(&str, Code); 5] = [
            ("vcs_claim_lost", Code::ClaimLost),
            ("vcs_generation_conflict", Code::GenerationConflict),
            ("unknown op", Code::UnknownOp),
            ("too_large", Code::ObjectTooLarge),
            ("vcs_not_found", Code::NotFound),
        ];
        TEXTS
            .into_iter()
            .find(|(text, _)| err.contains(text))
            .map(|(_, code)| code)
            .or_else(|| err.contains("not found").then_some(Self::NotFound))
            .unwrap_or_else(|| Self::Other(String::new()))
    }

    fn from_wire(code: &str) -> Self {
        match code {
            "invalid_request" => Self::InvalidRequest,
            "unknown_op" => Self::UnknownOp,
            "line_too_long" => Self::LineTooLong,
            "store_unavailable" => Self::StoreUnavailable,
            "internal_error" => Self::InternalError,
            "vcs_object_too_large" => Self::ObjectTooLarge,
            "vcs_not_found" => Self::NotFound,
            "vcs_generation_conflict" => Self::GenerationConflict,
            "vcs_claim_lost" => Self::ClaimLost,
            other => Self::Other(other.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireObject {
    #[serde(rename = "type")]
    pub kind: String,
    pub data: String,
}

/// One request of the control protocol. `traceparent` is added when the line is encoded.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Request {
    Get {
        sha: String,
    },
    Put {
        #[serde(rename = "type")]
        kind: String,
        data: String,
    },
    PutBatch {
        objects: Vec<WireObject>,
    },
    Has {
        shas: Vec<String>,
    },
    OpHeads {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expect: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        heads: Option<Vec<String>>,
    },
    Refs {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    SwapRef {
        name: String,
        expect: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit: Option<String>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        delete: bool,
    },
    PutOp {
        #[serde(rename = "type")]
        kind: String,
        sha: String,
        data: String,
    },
    GetOp {
        #[serde(rename = "type")]
        kind: String,
        sha: String,
    },
    GetBatch {
        shas: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_size: Option<u64>,
    },
    Workspace {
        sha: String,
        commit: String,
        generation: i64,
    },
    Changes,
    Dirty,
    Token,
    Stats,
}

impl Request {
    /// Whether sending the request twice has the effect of sending it once. A client resends only
    /// such a request after a lost answer.
    pub fn is_idempotent(&self) -> bool {
        !matches!(
            self,
            Self::SwapRef { .. } | Self::OpHeads { heads: Some(_), .. }
        )
    }
}

#[derive(Serialize)]
pub(super) struct Line<'a> {
    #[serde(flatten)]
    pub request: &'a Request,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct OpHeadsReply {
    #[serde(default, rename = "gen")]
    pub generation: i64,
    #[serde(default)]
    pub heads: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct TypeData {
    #[serde(rename = "type")]
    pub kind: String,
    pub data: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct Sha {
    pub sha: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct Shas {
    pub shas: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Present {
    pub present: Vec<bool>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Data {
    #[serde(default)]
    pub data: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct Datas {
    pub datas: Vec<String>,
    #[serde(default)]
    pub large: Vec<u64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct RefReply {
    #[serde(rename = "ref")]
    pub stored: super::RefInfo,
}

#[derive(Debug, Deserialize)]
pub(super) struct RefsReply {
    pub refs: Vec<super::RefInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The error answers of the released daemon, which sends no `code`.
    #[test]
    fn an_answer_without_a_code_maps_by_the_text_of_the_released_daemon() {
        for (err, want) in [
            ("unknown op", Code::UnknownOp),
            ("object not found", Code::NotFound),
            ("store: vcs_not_found", Code::NotFound),
            ("store: vcs_generation_conflict", Code::GenerationConflict),
            (
                "store: vfs: PUT /v1/vcs/refs/x: 409 Conflict: vcs_claim_lost",
                Code::ClaimLost,
            ),
            ("store: vcs_object_too_large", Code::ObjectTooLarge),
            ("boom", Code::Other(String::new())),
        ] {
            assert_eq!(Code::of_answer(None, err), want, "{err}");
        }
    }

    #[test]
    fn a_code_wins_over_the_text() {
        let got = Code::of_answer(Some("invalid_request"), "unknown op");
        assert_eq!(got, Code::InvalidRequest);
        assert_eq!(Code::of_answer(Some("x_new"), "").as_str(), "x_new");
    }
}
