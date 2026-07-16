use serde::Deserialize;
use std::collections::HashSet;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub listen: Option<String>,
    pub upstreams: Vec<UpstreamEntry>,
    pub fallback_ruleset: Option<String>,
    pub rulesets: Vec<Ruleset>,
    pub api_keys: Vec<ApiKeyEntry>,
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

#[derive(Debug, Clone, Deserialize)]
pub struct Rule {
    pub model: String,
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
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
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
                if rule.time_start >= rule.time_end {
                    return Err(
                        format!("ruleset '{}': time_start must be < time_end", rs.name).into(),
                    );
                }
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

fn rules_overlap(a: &Rule, b: &Rule) -> bool {
    let model_match = a.model == "*" || b.model == "*" || a.model == b.model;
    if !model_match {
        return false;
    }

    let days_a: HashSet<&str> = a.days.iter().map(|s| s.as_str()).collect();
    let days_b: HashSet<&str> = b.days.iter().map(|s| s.as_str()).collect();
    if days_a.intersection(&days_b).next().is_none() {
        return false;
    }

    a.time_start < b.time_end && b.time_start < a.time_end
}
