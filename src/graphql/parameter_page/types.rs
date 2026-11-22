use async_graphql::{
    ID, InputObject, OneofObject, Result, SimpleObject, Union,
};
use uuid::Uuid;

use super::page_error;

#[derive(Clone, Debug, PartialEq, SimpleObject)]
pub(in crate::graphql) struct ParameterPage {
    pub(super) id: ID,
    pub(super) title: String,
    pub(super) revision: String,
    sub_systems: Vec<ParameterPageSubSystem>,
}

impl ParameterPage {
    pub(super) fn new(id: ID, input: ParameterPageInput) -> Self {
        Self {
            id,
            title: input.title,
            revision: Uuid::new_v4().to_string(),
            sub_systems: input
                .sub_systems
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

#[derive(Debug, SimpleObject)]
pub(in crate::graphql) struct ParameterPageSummary {
    pub(super) id: ID,
    pub(super) title: String,
}

impl From<&ParameterPage> for ParameterPageSummary {
    fn from(page: &ParameterPage) -> Self {
        Self {
            id: page.id.clone(),
            title: page.title.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, SimpleObject)]
struct ParameterPageSubSystem {
    title: String,
    tabs: Vec<ParameterPageTab>,
}

#[derive(Clone, Debug, PartialEq, SimpleObject)]
struct ParameterPageTab {
    title: String,
    sub_pages: Vec<ParameterPageSubPage>,
}

#[derive(Clone, Debug, PartialEq, SimpleObject)]
struct ParameterPageSubPage {
    title: String,
    entries: Vec<ParameterPageEntry>,
}

#[derive(Clone, Debug, PartialEq, Union)]
enum ParameterPageEntry {
    Parameter(ParameterPageParameterEntry),
    Comment(ParameterPageCommentEntry),
    Mult(ParameterPageMultEntry),
}

#[derive(Clone, Debug, PartialEq, SimpleObject, InputObject)]
#[graphql(input_name = "ParameterPageParameterEntryInput")]
struct ParameterPageParameterEntry {
    drf: String,
    label: Option<String>,
    #[graphql(default = 1.0)]
    proportion: f64,
    #[graphql(default = true)]
    is_acnet: bool,
}

#[derive(Clone, Debug, PartialEq, SimpleObject, InputObject)]
#[graphql(input_name = "ParameterPageCommentEntryInput")]
struct ParameterPageCommentEntry {
    text: String,
}

#[derive(Clone, Debug, PartialEq, SimpleObject, InputObject)]
#[graphql(input_name = "ParameterPageMultEntryInput")]
struct ParameterPageMultEntry {
    /// Number of following rows to group with this entry.
    number_of_entries: i32,
    #[graphql(default)]
    description: String,
}

#[derive(Debug, InputObject)]
pub(in crate::graphql) struct ParameterPageInput {
    title: String,
    sub_systems: Vec<ParameterPageSubSystemInput>,
}

impl ParameterPageInput {
    pub(super) fn validate(&self) -> Result<()> {
        if self.sub_systems.is_empty() {
            return Err(page_error(
                "BAD_USER_INPUT",
                "A parameter page must have at least one subsystem.",
            ));
        }
        for system in &self.sub_systems {
            if system.tabs.is_empty() {
                return Err(page_error(
                    "BAD_USER_INPUT",
                    "Each subsystem must have at least one tab.",
                ));
            }
            for tab in &system.tabs {
                if tab.sub_pages.is_empty() {
                    return Err(page_error(
                        "BAD_USER_INPUT",
                        "Each tab must have at least one subpage.",
                    ));
                }
                for entry in tab.sub_pages.iter().flat_map(|page| &page.entries)
                {
                    if let ParameterPageEntryInput::Mult(mult) = entry
                        && mult.number_of_entries < 0
                    {
                        return Err(page_error(
                            "BAD_USER_INPUT",
                            "A Mult entry's numberOfEntries must be nonnegative.",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, InputObject)]
struct ParameterPageSubSystemInput {
    title: String,
    tabs: Vec<ParameterPageTabInput>,
}

impl From<ParameterPageSubSystemInput> for ParameterPageSubSystem {
    fn from(input: ParameterPageSubSystemInput) -> Self {
        Self {
            title: input.title,
            tabs: input.tabs.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, InputObject)]
struct ParameterPageTabInput {
    title: String,
    sub_pages: Vec<ParameterPageSubPageInput>,
}

impl From<ParameterPageTabInput> for ParameterPageTab {
    fn from(input: ParameterPageTabInput) -> Self {
        Self {
            title: input.title,
            sub_pages: input.sub_pages.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, InputObject)]
struct ParameterPageSubPageInput {
    title: String,
    entries: Vec<ParameterPageEntryInput>,
}

impl From<ParameterPageSubPageInput> for ParameterPageSubPage {
    fn from(input: ParameterPageSubPageInput) -> Self {
        Self {
            title: input.title,
            entries: input.entries.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, OneofObject)]
enum ParameterPageEntryInput {
    Parameter(ParameterPageParameterEntry),
    Comment(ParameterPageCommentEntry),
    Mult(ParameterPageMultEntry),
}

impl From<ParameterPageEntryInput> for ParameterPageEntry {
    fn from(input: ParameterPageEntryInput) -> Self {
        match input {
            ParameterPageEntryInput::Parameter(parameter) => {
                Self::Parameter(parameter)
            }
            ParameterPageEntryInput::Comment(comment) => Self::Comment(comment),
            ParameterPageEntryInput::Mult(mult) => Self::Mult(mult),
        }
    }
}
