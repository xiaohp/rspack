use rspack_core::{
  module_id, AsContextDependency, Compilation, Dependency, DependencyCategory, DependencyId,
  DependencyTemplate, DependencyType, ErrorSpan, ExtendedReferencedExport, ModuleDependency,
  ModuleGraph, RealDependencyLocation, RuntimeSpec, TemplateContext, TemplateReplaceSource,
};

#[derive(Debug, Clone)]
pub struct RequireResolveDependency {
  pub id: DependencyId,
  pub request: String,
  pub weak: bool,
  range: RealDependencyLocation,
  optional: bool,
}

impl RequireResolveDependency {
  pub fn new(request: String, range: RealDependencyLocation, weak: bool, optional: bool) -> Self {
    Self {
      range,
      request,
      weak,
      optional,
      id: DependencyId::new(),
    }
  }
}

impl Dependency for RequireResolveDependency {
  fn id(&self) -> &DependencyId {
    &self.id
  }

  fn category(&self) -> &DependencyCategory {
    &DependencyCategory::CommonJS
  }

  fn dependency_type(&self) -> &DependencyType {
    &DependencyType::RequireResolve
  }

  fn span(&self) -> Option<ErrorSpan> {
    Some(ErrorSpan::new(self.range.start, self.range.end))
  }

  fn get_referenced_exports(
    &self,
    _module_graph: &ModuleGraph,
    _runtime: Option<&RuntimeSpec>,
  ) -> Vec<ExtendedReferencedExport> {
    vec![]
  }

  fn could_affect_referencing_module(&self) -> rspack_core::AffectType {
    rspack_core::AffectType::True
  }
}

impl ModuleDependency for RequireResolveDependency {
  fn request(&self) -> &str {
    &self.request
  }

  fn user_request(&self) -> &str {
    &self.request
  }

  fn weak(&self) -> bool {
    self.weak
  }

  fn get_optional(&self) -> bool {
    self.optional
  }

  fn set_request(&mut self, request: String) {
    self.request = request;
  }
}

impl DependencyTemplate for RequireResolveDependency {
  fn apply(
    &self,
    source: &mut TemplateReplaceSource,
    code_generatable_context: &mut TemplateContext,
  ) {
    source.replace(
      self.range.start,
      self.range.end,
      module_id(
        code_generatable_context.compilation,
        &self.id,
        &self.request,
        self.weak,
      )
      .as_str(),
      None,
    );
  }

  fn dependency_id(&self) -> Option<DependencyId> {
    Some(self.id)
  }

  fn update_hash(
    &self,
    _hasher: &mut dyn std::hash::Hasher,
    _compilation: &Compilation,
    _runtime: Option<&RuntimeSpec>,
  ) {
  }
}

impl AsContextDependency for RequireResolveDependency {}
