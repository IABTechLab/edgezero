//! Keep the local check command aligned with the documented CI gates.

#[cfg(test)]
mod tests {
    fn gate_commands(markdown: &str) -> Vec<&str> {
        markdown
            .lines()
            .filter_map(|line| line.split_once(". `")?.1.strip_suffix('`'))
            .collect()
    }

    #[test]
    fn local_check_command_matches_documented_ci_gates() {
        let conventions = include_str!("../../../CLAUDE.md");
        let command = include_str!("../../../.claude/commands/check-ci.md");
        let gates = conventions
            .split_once("## CI Gates\n")
            .unwrap()
            .1
            .split_once("\n## ")
            .unwrap()
            .0;
        let expected = gate_commands(gates);
        assert_eq!(expected.len(), 6);
        assert_eq!(gate_commands(command), expected);
        assert!(command.starts_with(
            "Run the full CI gate checks locally before pushing. Run all six commands sequentially"
        ));
        let command_row = conventions
            .lines()
            .find(|line| line.starts_with("| `/check-ci`"))
            .unwrap();
        assert!(command_row.contains("Run all 6 CI gate checks locally"));
    }
}
