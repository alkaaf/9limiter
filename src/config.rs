use serde::Deserialize;
use std::collections::HashSet;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub listen: Option<String>,
    pub upstreams: Vec<UpstreamEntry>,
    pub fallback_ruleset: Option<String>,
    pub rulesets: Vec<Ruleset>,
    pub api_keys: Vec<ApiKeyEntry>,
    pub database: Option<DatabaseConfig>,
    pub timezone: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub dbname: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamEntry {
    pub path_prefix: String,
    pub base_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ruleset {
    pub name: String,
    pub rules: Vec<Rule>,
}

fn string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany { One(String), Many(Vec<String>) }
    match OneOrMany::deserialize(deserializer)? {
        OneOrMany::One(s) => Ok(vec![s]),
        OneOrMany::Many(v) => Ok(v),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Rule {
    #[serde(deserialize_with = "string_or_vec", alias = "model")]
    pub models: Vec<String>,
    pub limit: u32,
    pub window_secs: u64,
    pub time_start: String,
    pub time_end: String,
    pub days: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiKeyEntry {
    pub ruleset: String,
    pub keys: Vec<String>,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ruleset_names: HashSet<&str> =
            self.rulesets.iter().map(|r| r.name.as_str()).collect();

        if let Some(ref fallback) = self.fallback_ruleset {
            if !ruleset_names.contains(fallback.as_str()) {
                return Err(format!("fallback_ruleset '{}' not found in rulesets", fallback).into());
            }
        }

        for entry in &self.api_keys {
            if !ruleset_names.contains(entry.ruleset.as_str()) {
                return Err(format!("api_key ruleset '{}' not found", entry.ruleset).into());
            }
        }

        for rs in &self.rulesets {
            for rule in &rs.rules {
                if rule.limit == 0 {
                    return Err(format!("ruleset '{}': limit must be > 0", rs.name).into());
                }
                if rule.window_secs == 0 {
                    return Err(format!("ruleset '{}': window_secs must be > 0", rs.name).into());
                }
                for time in [&rule.time_start, &rule.time_end] {
                    let parts: Vec<&str> = time.split(':').collect();
                    if parts.len() != 2 {
                        return Err(
                            format!("ruleset '{}': invalid time format '{}'", rs.name, time).into(),
                        );
                    }
                    let h: u8 = parts[0]
                        .parse()
                        .map_err(|_| format!("invalid hour '{}'", parts[0]))?;
                    let m: u8 = parts[1]
                        .parse()
                        .map_err(|_| format!("invalid minute '{}'", parts[1]))?;
                    if h > 23 || m > 59 {
                        return Err(
                            format!("ruleset '{}': time '{}' out of range", rs.name, time).into(),
                        );
                    }
                }
                // Overnight window allowed (e.g. 22:00-07:00) — skip enforce
                // ponytail: overnight overlap detection is best-effort
                let valid_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
                for d in &rule.days {
                    if !valid_days.contains(&d.as_str()) {
                        return Err(format!("ruleset '{}': invalid day '{}'", rs.name, d).into());
                    }
                }
            }

            for i in 0..rs.rules.len() {
                for j in (i + 1)..rs.rules.len() {
                    if rules_overlap(&rs.rules[i], &rs.rules[j]) {
                        tracing::warn!(
                            "ruleset '{}': rules {} and {} may overlap",
                            rs.name,
                            i,
                            j
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

fn time_in_range(time: &str, start: &str, end: &str) -> bool {
    if start < end {
        time >= start && time < end
    } else {
        time >= start || time < end
    }
}

fn rules_overlap(a: &Rule, b: &Rule) -> bool {
    let model_match = a.models.iter().any(|m| m == "*")
        || b.models.iter().any(|m| m == "*")
        || a.models.iter().any(|m| b.models.contains(m));
    if !model_match {
        return false;
    }

    let days_a: HashSet<&str> = a.days.iter().map(|s| s.as_str()).collect();
    let days_b: HashSet<&str> = b.days.iter().map(|s| s.as_str()).collect();
    if days_a.intersection(&days_b).next().is_none() {
        return false;
    }

    // Check temporal overlap with overnight support
    time_in_range(&a.time_start, &b.time_start, &b.time_end)
        || time_in_range(&a.time_end, &b.time_start, &b.time_end)
        || time_in_range(&b.time_start, &a.time_start, &a.time_end)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── time_in_range ───────────────────────────────────────────────────────

    #[test]
    fn time_in_range_normal_window() {
        // Window 07:00-22:00
        assert!(time_in_range("07:00", "07:00", "22:00"), "start boundary");
        assert!(time_in_range("12:00", "07:00", "22:00"), "mid-window");
        assert!(time_in_range("21:59", "07:00", "22:00"), "before end");
        assert!(!time_in_range("06:59", "07:00", "22:00"), "before start");
        assert!(!time_in_range("22:00", "07:00", "22:00"), "at exclusive end");
        assert!(!time_in_range("23:00", "07:00", "22:00"), "after end");
    }

    #[test]
    fn time_in_range_overnight() {
        // Window 22:00-07:00
        assert!(time_in_range("22:00", "22:00", "07:00"), "start boundary");
        assert!(time_in_range("23:30", "22:00", "07:00"), "late night");
        assert!(time_in_range("00:00", "22:00", "07:00"), "midnight");
        assert!(time_in_range("06:59", "22:00", "07:00"), "before end");
        assert!(!time_in_range("07:00", "22:00", "07:00"), "at exclusive end");
        assert!(!time_in_range("10:00", "22:00", "07:00"), "gap outside window");
    }

    #[test]
    fn time_in_range_identical_start_end() {
        // 00:00-00:00 covers entire day (overnight path, always true)
        assert!(time_in_range("00:00", "00:00", "00:00"));
        assert!(time_in_range("12:00", "00:00", "00:00"));
        assert!(time_in_range("23:59", "00:00", "00:00"));
    }

    #[test]
    fn time_in_range_one_minute_window() {
        // Overnight comparison with single-minute window 23:59-00:00
        assert!(time_in_range("23:59", "23:59", "00:00"));
        assert!(!time_in_range("00:00", "23:59", "00:00"));
    }

    // ─── rules_overlap ───────────────────────────────────────────────────────

    fn make_rule(
        models: Vec<&str>,
        time_start: &str,
        time_end: &str,
        days: Vec<&str>,
    ) -> Rule {
        Rule {
            models: models.into_iter().map(String::from).collect(),
            limit: 10,
            window_secs: 3600,
            time_start: time_start.into(),
            time_end: time_end.into(),
            days: days.into_iter().map(String::from).collect(),
        }
    }

    #[test]
    fn rules_overlap_wildcard_matches_any_model() {
        let a = make_rule(vec!["*"], "09:00", "17:00", vec!["Mon"]);
        let b = make_rule(vec!["gpt-4"], "10:00", "16:00", vec!["Mon"]);
        assert!(rules_overlap(&a, &b));
        assert!(rules_overlap(&b, &a));
    }

    #[test]
    fn rules_overlap_exact_model_match() {
        let a = make_rule(vec!["gpt-4"], "09:00", "17:00", vec!["Mon"]);
        let b = make_rule(vec!["gpt-4"], "10:00", "16:00", vec!["Mon"]);
        assert!(rules_overlap(&a, &b));
    }

    #[test]
    fn rules_overlap_no_model_match() {
        let a = make_rule(vec!["gpt-4"], "09:00", "17:00", vec!["Mon"]);
        let b = make_rule(vec!["claude-3"], "09:00", "17:00", vec!["Mon"]);
        assert!(!rules_overlap(&a, &b));
    }

    #[test]
    fn rules_overlap_no_day_overlap() {
        let a = make_rule(vec!["*"], "09:00", "17:00", vec!["Mon"]);
        let b = make_rule(vec!["*"], "09:00", "17:00", vec!["Tue"]);
        assert!(!rules_overlap(&a, &b));
    }

    #[test]
    fn rules_overlap_different_days_same_model_no_overlap() {
        let a = make_rule(vec!["gpt-4"], "09:00", "17:00", vec!["Mon", "Wed"]);
        let b = make_rule(vec!["gpt-4"], "09:00", "17:00", vec!["Tue", "Thu"]);
        assert!(!rules_overlap(&a, &b));
    }

    #[test]
    fn rules_overlap_no_temporal_overlap() {
        let a = make_rule(vec!["*"], "09:00", "12:00", vec!["Mon"]);
        let b = make_rule(vec!["*"], "13:00", "17:00", vec!["Mon"]);
        assert!(!rules_overlap(&a, &b));
    }

    #[test]
    fn rules_overlap_edge_touching_overlaps_in_range_check() {
        // time_in_range uses >= for start, so a.end == b.start counts as overlap.
        // This is a best-effort warning — false positives are acceptable.
        let a = make_rule(vec!["*"], "09:00", "12:00", vec!["Mon"]);
        let b = make_rule(vec!["*"], "12:00", "17:00", vec!["Mon"]);
        assert!(rules_overlap(&a, &b));
    }

    #[test]
    fn rules_overlap_multi_model_partial_match() {
        let a = make_rule(vec!["gpt-4", "claude-3"], "09:00", "17:00", vec!["Mon"]);
        let b = make_rule(vec!["claude-3"], "10:00", "16:00", vec!["Mon"]);
        assert!(rules_overlap(&a, &b));
    }

    #[test]
    fn rules_overlap_both_wildcard() {
        let a = make_rule(vec!["*"], "09:00", "17:00", vec!["Mon"]);
        let b = make_rule(vec!["*"], "10:00", "16:00", vec!["Mon"]);
        assert!(rules_overlap(&a, &b));
    }

    #[test]
    fn rules_overlap_overnight_temporal() {
        let a = make_rule(vec!["*"], "22:00", "07:00", vec!["Mon"]);
        let b = make_rule(vec!["*"], "00:00", "05:00", vec!["Mon"]);
        assert!(rules_overlap(&a, &b));
    }

    #[test]
    fn rules_overlap_overnight_no_overlap_different_day() {
        let a = make_rule(vec!["*"], "22:00", "07:00", vec!["Mon"]);
        let b = make_rule(vec!["*"], "00:00", "05:00", vec!["Tue"]);
        assert!(!rules_overlap(&a, &b));
    }

    #[test]
    fn rules_overlap_empty_models_list() {
        let a = make_rule(vec![], "09:00", "17:00", vec!["Mon"]);
        let b = make_rule(vec!["gpt-4"], "09:00", "17:00", vec!["Mon"]);
        // Neither is "*" nor does a contain b's model
        assert!(!rules_overlap(&a, &b));
    }

    // ─── Config::validate ────────────────────────────────────────────────────

    fn valid_config() -> Config {
        Config {
            listen: None,
            upstreams: vec![],
            fallback_ruleset: Some("default".into()),
            rulesets: vec![Ruleset {
                name: "default".into(),
                rules: vec![Rule {
                    models: vec!["*".into()],
                    limit: 100,
                    window_secs: 3600,
                    time_start: "00:00".into(),
                    time_end: "23:59".into(),
                    days: vec![
                        "Mon".into(), "Tue".into(), "Wed".into(),
                        "Thu".into(), "Fri".into(), "Sat".into(), "Sun".into(),
                    ],
                }],
            }],
            api_keys: vec![],
            database: None,
            timezone: None,
        }
    }

    #[test]
    fn validate_ok() {
        assert!(valid_config().validate().is_ok());
    }

    #[test]
    fn validate_ok_no_fallback() {
        let mut cfg = valid_config();
        cfg.fallback_ruleset = None;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_ok_empty_rulesets() {
        let cfg = Config {
            listen: None,
            upstreams: vec![],
            fallback_ruleset: None,
            rulesets: vec![],
            api_keys: vec![],
            database: None,
            timezone: None,
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_err_fallback_not_found() {
        let mut cfg = valid_config();
        cfg.fallback_ruleset = Some("missing".into());
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("missing"));
        assert!(err.contains("rulesets"));
    }

    #[test]
    fn validate_err_api_key_ruleset_not_found() {
        let mut cfg = valid_config();
        cfg.api_keys = vec![ApiKeyEntry {
            ruleset: "ghost".into(),
            keys: vec!["sk-xxx".into()],
        }];
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("ghost"));
        assert!(err.contains("api_key"));
    }

    #[test]
    fn validate_err_limit_zero() {
        let mut cfg = valid_config();
        cfg.rulesets[0].rules[0].limit = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("limit must be > 0"));
    }

    #[test]
    fn validate_err_window_secs_zero() {
        let mut cfg = valid_config();
        cfg.rulesets[0].rules[0].window_secs = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("window_secs must be > 0"));
    }

    #[test]
    fn validate_err_invalid_time_format() {
        let mut cfg = valid_config();
        cfg.rulesets[0].rules[0].time_start = "nine".into();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("invalid time format"));
    }

    #[test]
    fn validate_err_hour_out_of_range() {
        let mut cfg = valid_config();
        cfg.rulesets[0].rules[0].time_start = "24:00".into();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("out of range") || err.contains("invalid hour"));
    }

    #[test]
    fn validate_err_minute_out_of_range() {
        let mut cfg = valid_config();
        cfg.rulesets[0].rules[0].time_start = "12:60".into();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("out of range") || err.contains("invalid minute"));
    }

    #[test]
    fn validate_err_time_not_numbers() {
        let mut cfg = valid_config();
        cfg.rulesets[0].rules[0].time_start = "ab:cd".into();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("invalid hour") || err.contains("parse"));
    }

    #[test]
    fn validate_err_invalid_day() {
        let mut cfg = valid_config();
        cfg.rulesets[0].rules[0].days = vec!["Funday".into()];
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("invalid day"));
    }

    #[test]
    fn validate_err_overnight_time_start_parses() {
        // Overnight window is allowed; test it parses correctly
        let mut cfg = valid_config();
        cfg.rulesets[0].rules[0].time_start = "22:00".into();
        cfg.rulesets[0].rules[0].time_end = "06:00".into();
        assert!(cfg.validate().is_ok(), "overnight window should be valid");
    }

    #[test]
    fn validate_multi_rule_overlap_warning_only() {
        // Overlapping rules only warn, not error
        let cfg = Config {
            listen: None,
            upstreams: vec![],
            fallback_ruleset: None,
            rulesets: vec![Ruleset {
                name: "test".into(),
                rules: vec![
                    Rule {
                        models: vec!["*".into()],
                        limit: 10,
                        window_secs: 3600,
                        time_start: "09:00".into(),
                        time_end: "17:00".into(),
                        days: vec!["Mon".into()],
                    },
                    Rule {
                        models: vec!["*".into()],
                        limit: 5,
                        window_secs: 1800,
                        time_start: "10:00".into(),
                        time_end: "16:00".into(),
                        days: vec!["Mon".into()],
                    },
                ],
            }],
            api_keys: vec![],
            database: None,
            timezone: None,
        };
        // Should still pass — overlap is only a warning
        assert!(cfg.validate().is_ok());
    }

    // ─── string_or_vec via Rule YAML deserialization ─────────────────────────

    #[test]
    fn string_or_vec_single_from_yaml() {
        let yaml = r#"
model: gpt-4
limit: 10
window_secs: 60
time_start: "09:00"
time_end: "17:00"
days: [Mon]
"#;
        let rule: Rule = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(rule.models, vec!["gpt-4"]);
    }

    #[test]
    fn string_or_vec_multi_from_yaml() {
        let yaml = r#"
models:
  - gpt-4
  - claude-3
limit: 10
window_secs: 60
time_start: "09:00"
time_end: "17:00"
days: [Mon]
"#;
        let rule: Rule = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(rule.models, vec!["gpt-4", "claude-3"]);
    }

    #[test]
    fn string_or_vec_empty_string() {
        let yaml = r#"
model: ""
limit: 10
window_secs: 60
time_start: "09:00"
time_end: "17:00"
days: [Mon]
"#;
        let rule: Rule = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(rule.models, vec![""]);
    }

    #[test]
    fn string_or_vec_single_with_alias() {
        // "models" is the canonical field name; "model" is alias for single string
        let yaml = r#"
model: claude-3-opus
limit: 5
window_secs: 120
time_start: "00:00"
time_end: "23:59"
days: [Mon, Tue, Wed]
"#;
        let rule: Rule = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(rule.models, vec!["claude-3-opus"]);
    }
}
