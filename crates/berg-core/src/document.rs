//! Generic presentation-neutral document model.
//!
//! Documents are structured content: titles, sections, paragraphs, tables,
//! properties, lists, and inline values. They intentionally do not know whether
//! they will become Markdown, ratatui widgets, HTML, or another output format.
//!
//! ## Module vocabulary
//!
//! - **document**: generic presentation-neutral model.
//! - **report**: Berg/Iceberg-specific builders that create documents.
//! - **render**: pure conversion from model to output format.
//! - **view**: final UI representation, especially TUI widgets/screens.

use time::OffsetDateTime;

/// Semantic document model shared by frontends and renderers.
///
/// Reports create documents from Berg/Iceberg data. Renderers and UI frontends
/// consume documents and decide how to present them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Document {
    /// Top-level document title.
    pub title: Cell,
    /// Ordered document blocks.
    pub blocks: Vec<Block>,
}

/// Block-level semantic content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Block {
    /// Paragraph-like inline content.
    Paragraph(Cell),
    /// Ordered key/value properties.
    Properties(Vec<Property>),
    /// Tabular content.
    Table(Table),
    /// Nested section.
    Section(Section),
    /// Ordered or unordered list.
    List(List),
    /// Fenced code block.
    FencedCode(FencedCode),
    /// Horizontal rule / thematic break.
    ThematicBreak,
}

/// Nested section with its own ordered blocks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    /// Section heading.
    pub title: Cell,
    /// Section body blocks.
    pub blocks: Vec<Block>,
}

/// Ordered or unordered list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct List {
    /// List marker style.
    pub kind: ListKind,
    /// Ordered list items.
    pub items: Vec<ListItem>,
}

/// List marker style.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListKind {
    /// Bullet list.
    Unordered,
    /// Numbered list.
    Ordered {
        /// First rendered number.
        start: usize,
    },
}

/// One list item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListItem {
    /// Item body blocks.
    pub blocks: Vec<Block>,
}

/// Semantic key/value property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Property {
    /// Property label.
    pub label: String,
    /// Property value.
    pub value: Cell,
}

/// Semantic table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    /// Ordered column labels.
    pub columns: Vec<Cell>,
    /// Ordered rows.
    pub rows: Vec<Row>,
}

/// Semantic table row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// Ordered row cells.
    pub cells: Vec<Cell>,
}

/// Inline content container used by titles, paragraphs, properties, lists, and tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cell {
    /// Ordered inline values.
    pub values: Vec<DocumentValue>,
}

impl Cell {
    /// Build a cell from inline values.
    #[must_use]
    pub fn new(values: Vec<DocumentValue>) -> Self {
        Self { values }
    }

    /// Build a plain-text cell.
    #[must_use]
    pub fn text(value: impl Into<String>) -> Self {
        Self::new(vec![DocumentValue::Text(value.into())])
    }

    /// Build a code-like cell.
    #[must_use]
    pub fn code(value: impl Into<String>) -> Self {
        Self::new(vec![DocumentValue::Code(value.into())])
    }

    /// Build a cell containing a single semantic value.
    #[must_use]
    pub fn value(value: DocumentValue) -> Self {
        Self::new(vec![value])
    }
}

/// Fenced code block content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FencedCode {
    /// Optional language tag.
    pub language: Option<String>,
    /// Code body.
    pub code: String,
}

/// Direction for a signed delta value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaDirection {
    /// Positive/additive delta.
    Positive,
    /// Negative/removal delta.
    Negative,
}

/// Category for an unknown value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownValueKind {
    /// Unknown value with no stronger type information.
    Generic,
    /// Unknown numeric value.
    Numeric,
}

/// Semantic status family for renderer-specific labels and styling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Confidence of a derived value.
    Confidence(ConfidenceStatus),
    /// Precision of a derived value.
    Precision(PrecisionStatus),
    /// Applicability of data to a decision or result.
    Applicability(ApplicabilityStatus),
    /// Completeness of analysis or reads.
    Completeness(CompletenessStatus),
    /// Compatibility between observed and expected types/values.
    Compatibility(CompatibilityStatus),
    /// Whether a requested operation or target is supported.
    Support(SupportStatus),
}

/// Confidence of a derived value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfidenceStatus {
    /// Fully supported by available metadata.
    High,
    /// Partially supported by available metadata.
    Partial,
    /// Valid but lowered by known delete impact.
    Lowered,
    /// Cannot be determined safely.
    Unknown,
    /// No value was available.
    Unavailable,
}

/// Precision of a derived value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrecisionStatus {
    /// Exact within the available evidence.
    Exact,
    /// Probably exact, but direct evidence is incomplete.
    ProbablyExact,
    /// May be truncated or approximate.
    PossiblyTruncated,
    /// Cannot be determined safely.
    Unknown,
    /// No value was available.
    Unavailable,
}

/// Applicability of one data set to another decision/result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicabilityStatus {
    /// Applies.
    Applies,
    /// Partially applies.
    PartiallyApplies,
    /// Does not apply.
    DoesNotApply,
    /// Applicability cannot be determined safely.
    Unknown,
}

/// Completeness of analysis or reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletenessStatus {
    /// Complete.
    Complete,
    /// Incomplete.
    Incomplete,
    /// Not applicable.
    NotApplicable,
}

/// Compatibility between observed and expected types/values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityStatus {
    /// Compatible without conversion.
    Compatible,
    /// Compatible through safe promotion.
    SafelyPromoted,
    /// Incompatible.
    Incompatible,
    /// Compatibility cannot be determined safely.
    Unknown,
}

/// Whether a requested operation or target is supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupportStatus {
    /// Supported.
    Supported,
    /// Unsupported.
    Unsupported,
}

/// Presence marker for matrix-like report cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
    /// Present.
    Present,
    /// Checked and absent.
    Absent,
    /// Not applicable for this row/column.
    NotApplicable,
}

/// Semantic inline value that renderers/frontends present in their own medium.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentValue {
    /// Plain text.
    Text(String),
    /// Code-like text, such as field paths, type names, or identifiers.
    Code(String),
    /// URI or URL value.
    Uri(String),
    /// Instant in time.
    Timestamp(OffsetDateTime),
    /// Instant in local time.
    LocalTimestamp(OffsetDateTime),
    /// Numeric value.
    Number(i64),
    /// Unsigned numeric value.
    Unsigned(u64),
    /// Byte size value.
    Bytes(u64),
    /// Signed delta value.
    Delta {
        /// Delta sign/direction.
        direction: DeltaDirection,
        /// Absolute delta magnitude, or missing when the source omitted it.
        value: Option<u64>,
    },
    /// Missing or unavailable value.
    MissingValue,
    /// Value exists conceptually, but could not be determined.
    UnknownValue {
        /// Known category for renderer behavior such as table alignment.
        kind: UnknownValueKind,
    },
    /// Semantic status.
    Status(Status),
    /// Presence marker for matrix-like report cells.
    Presence(Presence),
    /// Percentage stored as thousandths of one percent.
    PercentageMillis(u64),
    /// Non-negative count.
    Count(usize),
    /// Boolean value.
    Bool(bool),
    /// Emphasized inline values.
    Emphasis(Vec<DocumentValue>),
    /// Strongly emphasized inline values.
    Strong(Vec<DocumentValue>),
    /// Link with inline label and target URI.
    Link {
        /// Link label.
        label: Vec<DocumentValue>,
        /// Link target.
        target: String,
    },
    /// Image with alt text and source URI.
    Image {
        /// Image alt text.
        alt: String,
        /// Image source.
        source: String,
    },
    /// Hard line break.
    LineBreak,
}
