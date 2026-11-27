use super::*;

fn criteria(passed: &[&str]) -> Vec<u8> {
    let groups: &[(u8, &[&str])] = &[
        (1, &["slow-origin"]),
        (2, &["fast-origin"]),
        (3, &["cpu-stretch"]),
        (4, &["begin-progress"]),
        (5, &["batch-progress"]),
        (6, &["body-read-arrivals"]),
        (7, &["trickling-slot"]),
        (8, &["send-during-batch"]),
        (9, &["caps-while-waiting"]),
        (11, &["batch-write-deadline"]),
        (13, &["begin-upload-rules"]),
        (14, &["timeout-before-cutoff"]),
        (16, &["pending-head-cutoff", "pending-body-cutoff"]),
        (17, &["batch-cancel"]),
        (18, &["body-check"]),
        (19, &["post-delivery-batch"]),
        (20, &["response-handoff"]),
        (21, &super::response_cleanup::CASES),
        (
            21,
            &[
                "response-cleanup",
                "response-cleanup-source-error",
                "response-cleanup-write-deadline",
            ],
        ),
    ];
    let mut result = Vec::new();
    for (criterion, cases) in groups {
        if cases.iter().all(|case| passed.contains(case)) && !result.contains(criterion) {
            result.push(*criterion);
        }
    }
    result
}

pub(super) fn record(adapter: &str, passed: &[&str], records: &mut Records) -> Result<()> {
    for criterion in criteria(passed) {
        emit(adapter, criterion, records)?;
    }
    Ok(())
}

pub(super) fn emit(adapter: &str, criterion: u8, records: &mut Records) -> Result<()> {
    let environment = match adapter {
        "fastly" => "viceroy",
        "cloudflare" => "workerd",
        name => name,
    };
    let sentinel = format!("ez_6_acceptance_{criterion:02}_{environment}");
    records.push(json!({"event":"lifetime-acceptance","adapter":adapter,
        "case":format!("EZ-6/acceptance/{criterion:02}"),"sentinel":sentinel,
        "status":"pass","qualification":false}))?;
    println!("{sentinel}: passed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cutoff_acceptance_requires_both_pending_phases() {
        assert!(criteria(&["pending-head-cutoff"]).is_empty());
        assert!(criteria(&["pending-body-cutoff"]).is_empty());
        assert_eq!(
            criteria(&["pending-head-cutoff", "pending-body-cutoff"]),
            vec![16]
        );
    }

    #[test]
    fn response_cleanup_acceptance_requires_all_paths() {
        let cases = super::super::response_cleanup::CASES;
        for missing in 0..cases.len() {
            let partial: Vec<_> = cases
                .iter()
                .enumerate()
                .filter_map(|(index, case)| (index != missing).then_some(*case))
                .collect();
            assert!(!criteria(&partial).contains(&21));
        }
        assert_eq!(criteria(&cases), vec![21]);
    }

    #[test]
    fn unrelated_or_duplicate_passes_cannot_claim_other_criteria() {
        assert!(criteria(&["unknown", "unknown"]).is_empty());
        assert_eq!(criteria(&["slow-origin", "slow-origin"]), vec![1]);
        assert_eq!(
            criteria(&["slow-origin", "fast-origin", "timeout-before-cutoff"]),
            vec![1, 2, 14]
        );
    }
}
