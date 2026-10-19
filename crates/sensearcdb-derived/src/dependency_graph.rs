//! Dependency graph for derived tags.
//!
//! Maintains a DAG (Directed Acyclic Graph) of derived tag dependencies.
//! Provides Kahn topological sort for evaluation order and cycle detection.
//!
//! # Graph Structure
//!
//! - **nodes**: `(tenant_id, tag_name)` → `DerivedTagNode` (the tag's definition and metadata)
//! - **forward_edges**: `(tenant_id, source_tag)` → {tags that depend on it} (used for change propagation)
//! - **reverse_edges**: `(tenant_id, derived_tag)` → {its direct source tags} (used for topo sort)
//!
//! All keys are tenant-scoped: `tag_name` is unique within a tenant (A3 D4).
//! A derived tag's source references all belong to the same tenant as the tag
//! itself, so the tenant component of every edge key equals the node's tenant.
//!
//! # Cycle Detection
//!
//! Cycles are detected during `add_node` via incremental topological check.
//! A node cannot depend on itself (directly or transitively).

use crate::ast::ExprAst;
use crate::deadband::DeadbandConfig;
use crate::error::DerivedError;
use crate::fallback::FallbackPolicy;
use sensearcdb_common::{ColumnRef, TenantId};
use std::collections::{HashMap, HashSet, VecDeque};

/// Tenant-scoped key for a derived tag (and its source references).
pub type TagKey = (TenantId, String);

// ─── DerivedTagNode ───

/// A single node in the dependency graph representing a derived tag.
#[derive(Debug, Clone)]
pub struct DerivedTagNode {
    /// Table ID the derived tag writes to.
    pub table_id: u32,
    /// Tag name (租户内唯一).
    pub name: String,
    /// Parsed expression AST.
    pub ast: ExprAst,
    /// Source tag names (extracted from `ast`).
    pub source_tags: Vec<String>,
    /// Resolved source column references, aligned one-to-one with `source_tags`.
    ///
    /// Each entry is `(source_tag_name, ColumnRef)`. Because the engine supports
    /// cross-table source tags, the table ID of a source may differ from this
    /// node's own `table_id`; the `column_id` selects a specific wide-table
    /// column (`0` for the N=1 single-column case).
    pub source_table_ids: Vec<(String, ColumnRef)>,
    /// Current node state.
    pub state: NodeState,
    /// FNV-1a hash of the expression string (for change detection).
    pub expression_hash: u64,
    /// Fallback policy when source data is unavailable.
    pub fallback_policy: FallbackPolicy,
    /// Deadband filter configuration.
    pub deadband: DeadbandConfig,
    /// Timestamp of the last source value that triggered a recalculation.
    pub last_source_max_ts: i64,
}

impl DerivedTagNode {
    /// Create a new derived tag node.
    pub fn new(
        table_id: u32,
        name: String,
        ast: ExprAst,
        expression_hash: u64,
        fallback_policy: FallbackPolicy,
        deadband: DeadbandConfig,
        source_table_ids: Vec<(String, ColumnRef)>,
    ) -> Self {
        let source_tags = ast.extract_source_tags();
        Self {
            table_id,
            name,
            ast,
            source_tags,
            source_table_ids,
            state: NodeState::Active,
            expression_hash,
            fallback_policy,
            deadband,
            last_source_max_ts: 0,
        }
    }
}

// ─── NodeState ───

/// Operational state of a derived tag node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    /// Node is actively being computed.
    Active,
    /// Node has been defined but source tags have no data yet.
    Pending,
    /// Cold-start: source data may be stale.
    Stale,
    /// Node is disabled (user action or cascade).
    Disabled,
    /// Node is in an error state.
    Error(String),
}

// ─── DependencyGraph ───

/// The dependency graph tracking all derived tags and their inter-dependencies.
pub struct DependencyGraph {
    /// (tenant_id, tag_name) → node definition
    nodes: HashMap<TagKey, DerivedTagNode>,
    /// (tenant_id, source_tag) → {dependent derived tags}
    forward_edges: HashMap<TagKey, HashSet<TagKey>>,
    /// (tenant_id, derived_tag) → {its direct source tags}
    reverse_edges: HashMap<TagKey, HashSet<TagKey>>,
    /// Cached topological order; invalidated on graph mutation.
    topo_order: Option<Vec<TagKey>>,
}

impl DependencyGraph {
    /// Create an empty dependency graph.
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            forward_edges: HashMap::new(),
            reverse_edges: HashMap::new(),
            topo_order: None,
        }
    }

    /// Return the number of nodes in the graph.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Return `true` if the graph is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Get a node by (tenant_id, name).
    pub fn get(&self, tenant_id: TenantId, name: &str) -> Option<&DerivedTagNode> {
        self.nodes.get(&(tenant_id, name.to_string()))
    }

    /// Get a mutable reference to a node by (tenant_id, name).
    pub fn get_mut(&mut self, tenant_id: TenantId, name: &str) -> Option<&mut DerivedTagNode> {
        self.nodes.get_mut(&(tenant_id, name.to_string()))
    }

    /// Check if a node exists (within the given tenant).
    pub fn contains(&self, tenant_id: TenantId, name: &str) -> bool {
        self.nodes.contains_key(&(tenant_id, name.to_string()))
    }

    /// Return all node names for a given tenant（读侧隔离）.
    pub fn node_names_for_tenant(&self, tenant_id: TenantId) -> Vec<&String> {
        self.nodes
            .keys()
            .filter(|k| k.0 == tenant_id)
            .map(|k| &k.1)
            .collect()
    }

    /// Return all node names across every tenant（gRPC admin / 回放等未挂载或非读面用）.
    pub fn all_node_names(&self) -> Vec<&String> {
        self.nodes.keys().map(|k| &k.1).collect()
    }

    /// Get the source tags (direct dependencies) of a node.
    pub fn get_source_tags(&self, tenant_id: TenantId, name: &str) -> Option<&HashSet<TagKey>> {
        self.reverse_edges.get(&(tenant_id, name.to_string()))
    }

    /// Export a snapshot of all node definitions (cloned).
    ///
    /// Used by the replay subsystem to freeze the derived-tag definitions for a
    /// session. The returned nodes carry the resolved `source_table_ids` needed
    /// for cross-table as-of joins (derived-on-derived included). Iteration order
    /// is unspecified; pair with [`DependencyGraph::topological_sort`] for a
    /// dependency-ordered evaluation.
    pub fn export_nodes(&self) -> Vec<DerivedTagNode> {
        self.nodes.values().cloned().collect()
    }

    // ─── Graph Mutation ───

    /// Validate (dry-run) that adding `node` would not violate graph invariants.
    ///
    /// This is a pure, non-mutating check used by
    /// [`DerivedTagEngine::register_tag`] to move every fallible validation
    /// **before** persistence, so a failed validation can never leave the store
    /// polluted with a definition that would later be dropped by
    /// [`DependencyGraph::add_node`] (see the persist-first "库新内存旧" window).
    ///
    /// Mirrors the validations performed by `add_node`:
    /// 1. Node count does not exceed `max_nodes` (except when replacing an
    ///    existing node under the same key).
    /// 2. No self-reference.
    /// 3. No transitive cycle is introduced.
    ///
    /// # Errors
    /// Returns [`DerivedError::TooManyNodes`] or [`DerivedError::CircularDependency`].
    pub fn validate_node(
        &self,
        tenant_id: TenantId,
        node: &DerivedTagNode,
        max_nodes: usize,
    ) -> Result<(), DerivedError> {
        let key: TagKey = (tenant_id, node.name.clone());

        if self.nodes.len() >= max_nodes && !self.nodes.contains_key(&key) {
            return Err(DerivedError::TooManyNodes {
                count: self.nodes.len() + 1,
                max_nodes,
            });
        }

        // Build tentative reverse edges (all sources share the node's tenant).
        let source_set: HashSet<TagKey> = node
            .source_tags
            .iter()
            .map(|s| (tenant_id, s.clone()))
            .collect();

        // Check for self-reference
        if source_set.contains(&key) {
            return Err(DerivedError::CircularDependency {
                cycle: vec![node.name.clone(), node.name.clone()],
            });
        }

        // Check for cycles by doing a DFS from each source
        // A cycle exists if name can be reached from any of its sources
        // (i.e., if some source transitively depends on name)
        for source in &source_set {
            if self.transitively_depends_on(source, &key) {
                return Err(DerivedError::CircularDependency {
                    cycle: self
                        .find_cycle(source, &key)
                        .into_iter()
                        .map(|k| k.1)
                        .collect(),
                });
            }
        }

        Ok(())
    }

    /// Add a node to the dependency graph.
    ///
    /// Validates that:
    /// 1. No cycle is introduced.
    /// 2. The node count does not exceed `max_nodes`.
    /// 3. Source tags are valid (either already in graph or will be fed externally).
    ///
    /// # Errors
    /// Returns [`DerivedError::CircularDependency`] if adding this node creates a cycle.
    /// Returns [`DerivedError::TooManyNodes`] if the graph exceeds `max_nodes`.
    pub fn add_node(
        &mut self,
        tenant_id: TenantId,
        node: DerivedTagNode,
        max_nodes: usize,
    ) -> Result<(), DerivedError> {
        // Perform all fallible validations first (dry-run, no mutation). This is
        // a superset of what `register_tag` already ran pre-persist, and it
        // re-guards against the tiny TOCTOU window between pre-persist validation
        // and this write.
        self.validate_node(tenant_id, &node, max_nodes)?;

        let key: TagKey = (tenant_id, node.name.clone());

        // Build tentative reverse edges (all sources share the node's tenant).
        let source_set: HashSet<TagKey> = node
            .source_tags
            .iter()
            .map(|s| (tenant_id, s.clone()))
            .collect();

        // Update forward edges for each source
        for source in &source_set {
            self.forward_edges
                .entry(source.clone())
                .or_default()
                .insert(key.clone());
        }

        // Store reverse edges
        self.reverse_edges.insert(key.clone(), source_set);

        // Remove old forward edges if replacing a node
        if let Some(old_node) = self.nodes.get(&key) {
            for old_source in &old_node.source_tags {
                let old_key: TagKey = (tenant_id, old_source.clone());
                if let Some(deps) = self.forward_edges.get_mut(&old_key) {
                    deps.remove(&key);
                }
            }
        }

        self.nodes.insert(key, node);
        self.topo_order = None; // invalidate cache

        Ok(())
    }

    /// Remove a node and return the names of all downstream nodes
    /// that should be cascade-disabled.
    pub fn remove_node(&mut self, tenant_id: TenantId, name: &str) -> Vec<String> {
        let key: TagKey = (tenant_id, name.to_string());

        // Find all downstream nodes (transitive dependents)
        let affected = self.get_transitive_derived_tags(&[key.clone()]);

        // Remove forward edges
        for source in &self.reverse_edges.remove(&key).unwrap_or_default() {
            if let Some(deps) = self.forward_edges.get_mut(source) {
                deps.remove(&key);
            }
        }

        // Remove node
        self.nodes.remove(&key);
        self.topo_order = None;

        affected.into_iter().map(|k| k.1).collect()
    }

    /// Update a node's state.
    pub fn set_state(&mut self, tenant_id: TenantId, name: &str, state: NodeState) {
        if let Some(node) = self.nodes.get_mut(&(tenant_id, name.to_string())) {
            node.state = state;
        }
    }

    /// Update a node's `last_source_max_ts`.
    pub fn set_last_source_max_ts(&mut self, tenant_id: TenantId, name: &str, ts: i64) {
        if let Some(node) = self.nodes.get_mut(&(tenant_id, name.to_string())) {
            node.last_source_max_ts = ts;
        }
    }

    // ─── Topological Sort ───

    /// Perform a Kahn topological sort over all nodes.
    ///
    /// Returns nodes in dependency order (sources before dependents).
    /// Returned names are projected from the tenant-scoped keys（供回放等非读面路径）.
    ///
    /// # Errors
    /// Returns `Err(cycle_names)` if a cycle is detected.
    pub fn topological_sort(&self) -> Result<Vec<String>, Vec<String>> {
        if let Some(ref cached) = self.topo_order {
            return Ok(cached.iter().map(|k| k.1.clone()).collect());
        }

        // Compute in-degree for each node
        let mut in_degree: HashMap<TagKey, usize> = HashMap::new();
        for name in self.nodes.keys() {
            in_degree.insert(name.clone(), 0);
        }
        for (name, sources) in &self.reverse_edges {
            let deg = in_degree.entry(name.clone()).or_insert(0);
            for source in sources {
                if self.nodes.contains_key(source) {
                    *deg += 1;
                }
            }
        }

        // Queue nodes with in-degree 0
        let mut queue: VecDeque<TagKey> = VecDeque::new();
        for (name, deg) in &in_degree {
            if *deg == 0 {
                queue.push_back(name.clone());
            }
        }

        let mut sorted: Vec<TagKey> = Vec::new();
        while let Some(name) = queue.pop_front() {
            sorted.push(name.clone());
            // Decrement in-degree of dependents
            if let Some(dependents) = self.forward_edges.get(&name) {
                for dep in dependents {
                    if let Some(deg) = in_degree.get_mut(dep) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(dep.clone());
                        }
                    }
                }
            }
        }

        // Check for cycles
        if sorted.len() != self.nodes.len() {
            let sorted_set: HashSet<&TagKey> = sorted.iter().collect();
            let remaining: Vec<String> = self
                .nodes
                .keys()
                .filter(|n| !sorted_set.contains(n))
                .map(|n| n.1.clone())
                .collect();
            return Err(remaining);
        }

        Ok(sorted.into_iter().map(|k| k.1).collect())
    }

    /// Perform a topological sort over a subset of nodes.
    ///
    /// The result only contains nodes from `affected` and their
    /// intermediate dependencies (within `self.nodes`).
    pub fn topological_subset(&self, affected: &HashSet<TagKey>) -> Vec<TagKey> {
        // Compute in-degree only for affected nodes (edges that cross into affected)
        let mut in_degree: HashMap<TagKey, usize> = HashMap::new();
        let mut affected_deps: HashMap<TagKey, Vec<TagKey>> = HashMap::new();

        for name in affected {
            in_degree.insert(name.clone(), 0);
            if let Some(sources) = self.reverse_edges.get(name) {
                let count = sources.iter().filter(|s| affected.contains(*s)).count();
                *in_degree.get_mut(name).unwrap() = count;
                for s in sources {
                    if affected.contains(s) {
                        affected_deps
                            .entry(s.clone())
                            .or_default()
                            .push(name.clone());
                    }
                }
            }
        }

        // Queue nodes with in-degree 0
        let mut queue: VecDeque<TagKey> = VecDeque::new();
        for (name, deg) in &in_degree {
            if *deg == 0 {
                queue.push_back(name.clone());
            }
        }

        let mut sorted: Vec<TagKey> = Vec::new();
        while let Some(name) = queue.pop_front() {
            sorted.push(name.clone());
            if let Some(deps) = affected_deps.get(&name) {
                for dep in deps {
                    if let Some(deg) = in_degree.get_mut(dep) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(dep.clone());
                        }
                    }
                }
            }
        }

        sorted
    }

    // ─── Change Propagation ───

    /// Get all derived tags that transitively depend on any of the given `source_keys`.
    ///
    /// This includes:
    /// - Tags that directly reference `source_keys`
    /// - Tags that depend on those tags, and so on
    ///
    /// `source_keys` are tenant-scoped `(tenant_id, source_tag_name)`.
    pub fn get_transitive_derived_tags(&self, source_keys: &[TagKey]) -> HashSet<TagKey> {
        let mut result: HashSet<TagKey> = HashSet::new();
        let mut queue: VecDeque<TagKey> = VecDeque::new();

        for key in source_keys {
            if let Some(direct) = self.forward_edges.get(key) {
                for dep in direct {
                    if result.insert(dep.clone()) {
                        queue.push_back(dep.clone());
                    }
                }
            }
        }

        // BFS to find transitive dependents
        while let Some(current) = queue.pop_front() {
            if let Some(next_level) = self.forward_edges.get(&current) {
                for dep in next_level {
                    if result.insert(dep.clone()) {
                        queue.push_back(dep.clone());
                    }
                }
            }
        }

        result
    }

    // ─── Internal Helpers ───

    /// Check if `source` transitively depends on `target`.
    fn transitively_depends_on(&self, source: &TagKey, target: &TagKey) -> bool {
        if source == target {
            return true;
        }
        let mut visited: HashSet<TagKey> = HashSet::new();
        let mut stack = vec![source.clone()];
        while let Some(current) = stack.pop() {
            if !visited.insert(current.clone()) {
                continue;
            }
            if &current == target {
                return true;
            }
            if let Some(sources) = self.reverse_edges.get(&current) {
                for s in sources {
                    stack.push(s.clone());
                }
            }
        }
        false
    }

    /// Find a cycle path from `source` to `target`.
    fn find_cycle(&self, source: &TagKey, target: &TagKey) -> Vec<TagKey> {
        // Simple DFS to find one cycle
        let mut path = vec![source.clone()];
        let mut visited: HashSet<TagKey> = HashSet::new();
        visited.insert(source.clone());

        if self.dfs_find_cycle(source, target, &mut path, &mut visited) {
            path.push(target.clone());
            path
        } else {
            vec![source.clone(), target.clone()]
        }
    }

    fn dfs_find_cycle(
        &self,
        current: &TagKey,
        target: &TagKey,
        path: &mut Vec<TagKey>,
        visited: &mut HashSet<TagKey>,
    ) -> bool {
        if current == target {
            return true;
        }
        if let Some(sources) = self.reverse_edges.get(current) {
            for source in sources {
                if visited.insert(source.clone()) {
                    path.push(source.clone());
                    if self.dfs_find_cycle(source, target, path, visited) {
                        return true;
                    }
                    path.pop();
                }
            }
        }
        false
    }
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::ExprAst;
    use crate::lexer::Lexer;
    use crate::parser::PrattParser;

    const T: TenantId = TenantId::DEFAULT;

    fn make_node(name: &str, expr: &str) -> DerivedTagNode {
        let mut lexer = Lexer::new(expr);
        let tokens = lexer.tokenize().unwrap();
        let mut parser = PrattParser::new(tokens);
        let ast = parser.parse_expression().unwrap();
        // Test helper: resolve every source tag to a placeholder table ID.
        let source_table_ids = ast
            .extract_source_tags()
            .into_iter()
            .map(|s| (s, ColumnRef::single(999u32)))
            .collect();
        DerivedTagNode::new(
            1,
            name.to_string(),
            ast,
            0,
            FallbackPolicy::Strict,
            DeadbandConfig::default(),
            source_table_ids,
        )
    }

    #[test]
    fn test_add_node_simple() {
        let mut graph = DependencyGraph::new();
        let node = make_node("derived_a", "src_1 + src_2");
        graph.add_node(T, node, 100).unwrap();
        assert_eq!(graph.len(), 1);
        assert!(graph.contains(T, "derived_a"));
    }

    #[test]
    fn test_same_name_across_tenants() {
        let mut graph = DependencyGraph::new();
        let a = make_node("dup", "src_a");
        let b = make_node("dup", "src_b");
        graph.add_node(TenantId(1), a, 100).unwrap();
        graph.add_node(TenantId(2), b, 100).unwrap();
        assert_eq!(graph.len(), 2);
        assert!(graph.contains(TenantId(1), "dup"));
        assert!(graph.contains(TenantId(2), "dup"));
        assert_eq!(graph.node_names_for_tenant(TenantId(1)).len(), 1);
        assert_eq!(graph.node_names_for_tenant(TenantId(2)).len(), 1);
    }

    #[test]
    fn test_topological_sort_simple() {
        let mut graph = DependencyGraph::new();
        // derived_b depends on derived_a, derived_a depends on src_1
        let node_a = make_node("derived_a", "src_1 * 2");
        graph.add_node(T, node_a, 100).unwrap();

        let node_b = make_node("derived_b", "derived_a + 1");
        graph.add_node(T, node_b, 100).unwrap();

        let order = graph.topological_sort().unwrap();
        // derived_a must come before derived_b
        let pos_a = order.iter().position(|n| n == "derived_a").unwrap();
        let pos_b = order.iter().position(|n| n == "derived_b").unwrap();
        assert!(pos_a < pos_b);
    }

    #[test]
    fn test_cycle_detection_direct() {
        let mut graph = DependencyGraph::new();
        // derived_a depends on derived_a → should fail
        let node = DerivedTagNode::new(
            1,
            "derived_a".into(),
            ExprAst::TagRef {
                name: "derived_a".into(),
            },
            0,
            FallbackPolicy::Strict,
            DeadbandConfig::default(),
            vec![("derived_a".to_string(), ColumnRef::single(1))],
        );
        let result = graph.add_node(T, node, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_cycle_detection_indirect() {
        let mut graph = DependencyGraph::new();
        let node_a = make_node("derived_a", "derived_b");
        graph.add_node(T, node_a, 100).unwrap();

        // derived_b depends on derived_a → creates a cycle
        let node_b = make_node("derived_b", "derived_a");
        let result = graph.add_node(T, node_b, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_topological_sort_cycle() {
        // Manually break the graph interface to test cycle detection in sort
        let mut graph = DependencyGraph::new();
        let node_a = make_node("a", "b");
        let node_b = make_node("b", "c");
        graph.add_node(T, node_a, 100).unwrap();
        graph.add_node(T, node_b, 100).unwrap();
        // Add c by manually breaking invariant
        let node_c = make_node("c", "a");
        graph.nodes.insert((T, "c".into()), node_c);
        graph
            .reverse_edges
            .insert((T, "c".into()), [(T, "a".into())].into());

        let result = graph.topological_sort();
        assert!(result.is_err());
    }

    #[test]
    fn test_transitive_derived_tags() {
        let mut graph = DependencyGraph::new();
        // Chain: src → a → b → c
        let a = make_node("a", "src * 2");
        let b = make_node("b", "a + 1");
        let c = make_node("c", "b / 2");

        graph.add_node(T, a, 100).unwrap();
        graph.add_node(T, b, 100).unwrap();
        graph.add_node(T, c, 100).unwrap();

        let affected = graph.get_transitive_derived_tags(&[(T, "src".into())]);
        assert_eq!(affected.len(), 3);
        assert!(affected.contains(&(T, "a".into())));
        assert!(affected.contains(&(T, "b".into())));
        assert!(affected.contains(&(T, "c".into())));
    }

    #[test]
    fn test_remove_node() {
        let mut graph = DependencyGraph::new();
        let a = make_node("a", "src");
        let b = make_node("b", "a");

        graph.add_node(T, a, 100).unwrap();
        graph.add_node(T, b, 100).unwrap();

        let cascade = graph.remove_node(T, "a");
        assert!(cascade.contains(&"b".to_string()));
        assert!(!graph.contains(T, "a"));
    }

    #[test]
    fn test_topological_subset() {
        let mut graph = DependencyGraph::new();
        let a = make_node("a", "src1");
        let b = make_node("b", "a");
        let c = make_node("c", "src2");
        let d = make_node("d", "b + c");

        graph.add_node(T, a, 100).unwrap();
        graph.add_node(T, b, 100).unwrap();
        graph.add_node(T, c, 100).unwrap();
        graph.add_node(T, d, 100).unwrap();

        let affected: HashSet<TagKey> = [(T, "b".into()), (T, "d".into()), (T, "c".into())]
            .into_iter()
            .collect();
        let order = graph.topological_subset(&affected);

        // b must come before d (b → d)
        let pos_b = order.iter().position(|n| n.1 == "b").unwrap();
        let pos_d = order.iter().position(|n| n.1 == "d").unwrap();
        assert!(pos_b < pos_d);
    }

    #[test]
    fn test_max_nodes() {
        let mut graph = DependencyGraph::new();
        let node1 = make_node("n1", "src1");
        graph.add_node(T, node1, 1).unwrap(); // max = 1

        let node2 = make_node("n2", "src2");
        let result = graph.add_node(T, node2, 1);
        assert!(result.is_err());
    }
}
