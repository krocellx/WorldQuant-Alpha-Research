import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv

from src.utilities.parameters import BASE_DIR
from src.core.wq_alpha_analysis import AlphaTracker

load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env.local"))  # Load environment variables

class AlphaTrackerConfig:
    ALPHA_DIR = os.getenv("ALPHA_DIR", os.path.join(BASE_DIR, 'iqc_alpha'))  # Default path if not set in .env
    EDIT_COLS = ['link', 'passed_checks', 'failed', 'sharpe', 'fitness', 'turnover', 'weight_check', 'subsharpe',
                 'correlation', 'code', 'manual_reviewed', 'submitted', 'note_1', 'note_2', 'idea_id']

    DROPDOWN_OPTIONS = {
        'universe': ['TOP3000', 'TOP1000', 'TOP500', 'TOP200', 'TOPSP500'],
        'region': ['USA'],
        'neutralization': ['SUBINDUSTRY', 'INDUSTRY', 'SECTOR', 'MARKET', 'NONE']
    }

    # Fields that should be rendered as checkboxes
    CHECKBOX_FIELDS = ['manual_reviewed', 'submitted']

    # Default filter values
    DEFAULT_MIN_SHARPE = 1.4
    DEFAULT_HIDE_REVIEWED = True


class AlphaDataHandler:
    def __init__(self, config):
        self.config = config

    def load_object(self, file_name):
        try:
            file_path = os.path.join(self.config.ALPHA_DIR, file_name)
            alpha_obj = AlphaTracker(tracker_file=file_path)
            return alpha_obj, file_path
        except Exception as e:
            st.error(f"Error loading file: {e}")
            return None, None

    def apply_filters(self, df, min_sharpe, hide_reviewed):
        filtered_df = df.copy()
        filtered_df = filtered_df[filtered_df["sharpe"] >= min_sharpe]
        if hide_reviewed:
            filtered_df = filtered_df[filtered_df["manual_reviewed"] == False]
        return filtered_df.sort_values(by='sharpe', ascending=False).reset_index(drop=True)

class AlphaTrackerUI:
    def __init__(self, config, data_handler):
        self.config = config
        self.data_handler = data_handler

    def sidebar(self):
        st.sidebar.title("Alpha File Loader")

        # --- SIDEBAR: File Selection and Loading ---
        files = [f for f in os.listdir(self.config.ALPHA_DIR) if f.endswith(".csv")]
        selected_file = st.sidebar.selectbox("Select Alpha Tracker File", files)
        if st.sidebar.button("Load File"):
            alpha_obj, file_path = self.data_handler.load_object(selected_file)
            if alpha_obj is not None:
                st.session_state["alpha_obj"] = alpha_obj
                st.session_state["file_loaded"] = selected_file
                st.session_state["file_path"] = file_path

        # --- SIDEBAR: Filter Options ---
        st.sidebar.markdown("---")
        st.sidebar.subheader("View Filters")

        min_sharpe = st.sidebar.slider("Minimum Sharpe",
                                       min_value=-5.0,
                                       max_value=5.0,
                                       value=self.config.DEFAULT_MIN_SHARPE,
                                       step=0.1)
        st.session_state["min_sharpe"] = min_sharpe

        hide_reviewed = st.sidebar.checkbox("Hide Manually Reviewed",
                                            value=self.config.DEFAULT_HIDE_REVIEWED)
        st.session_state["hide_reviewed"] = hide_reviewed

    def create_column_config(self):
        column_config = {}

        # Add dropdown options
        for col, options in self.config.DROPDOWN_OPTIONS.items():
            column_config[col] = st.column_config.SelectboxColumn(
                col.replace('_', ' ').title(),
                options=options
            )

        # Add checkbox fields
        for col in self.config.CHECKBOX_FIELDS:
            column_config[col] = st.column_config.CheckboxColumn(
                col.replace('_', ' ').title()
            )

        # Make idea_id non-editable
        column_config["idea_id"] = st.column_config.TextColumn(
            "Idea ID",
            disabled=True
        )

        return column_config


    def main(self):
        # --- MAIN PAGE ---
        st.title("Alpha Tagging Tool")

        if "file_loaded" not in st.session_state:
            st.info("Please select and load a file from the sidebar.")
            return

        st.success(f"Loaded: {st.session_state['file_loaded']}")
        df = st.session_state["alpha_obj"].df.copy()

        # Get column order
        all_cols = self.config.EDIT_COLS + [col for col in df.columns if col not in self.config.EDIT_COLS]

        # Apply filters
        min_sharpe = st.session_state.get("min_sharpe", self.config.DEFAULT_MIN_SHARPE)
        hide_reviewed = st.session_state.get("hide_reviewed", self.config.DEFAULT_HIDE_REVIEWED)
        view_df = self.data_handler.apply_filters(df[all_cols], min_sharpe, hide_reviewed)

        # Store original view for change tracking
        st.session_state["original_view_df"] = view_df.copy()

        # Create column config for dropdowns
        column_config = self.create_column_config()

        # Data editor with dropdowns
        edited_df = st.data_editor(
            view_df,
            column_config=column_config,
            num_rows="dynamic",
            use_container_width=True
        )

        self.render_action_buttons(edited_df, all_cols)

    def render_action_buttons(self, edited_df, all_cols):
        if st.button("Apply Changes to Full DataFrame"):
            self.apply_changes(edited_df, all_cols)

        if st.button("Save Changes"):
            try:
                st.session_state["alpha_obj"].save_tracker()
                st.success("Changes saved successfully!")
            except Exception as e:
                st.error(f"Error saving file: {e}")

    def apply_changes(self, edited_df, all_cols):
        changed_df = edited_df.compare(st.session_state["original_view_df"], keep_equal=False)
        changed_rows = changed_df.index.get_level_values(0).unique()
        idea_id_lst = []

        # Process existing row updates
        if not changed_rows.empty:
            for row_idx in changed_rows:
                edited_row = edited_df.iloc[row_idx]
                idea_id = edited_row["idea_id"]
                idea_id_lst.append(idea_id)

                # Create a dictionary of fields to update
                updates = {col: edited_row[col] for col in all_cols if col in edited_row}

                # Update all columns at once
                st.session_state["alpha_obj"].update_idea_batch(idea_id, updates)

            st.success(f"Updated {len(changed_rows)} row(s) in full_df.")
            with st.expander("🔍 View Changed Rows"):
                affected_rows = st.session_state["alpha_obj"].df[
                    st.session_state["alpha_obj"].df["idea_id"].isin(idea_id_lst)]
                st.dataframe(affected_rows)

        # Handle new rows (rows that don't exist in original_view_df)
        new_rows = []
        for i, row in edited_df.iterrows():
            # Check if this is a new row (empty or default idea_id)
            if pd.isna(row["idea_id"]) or row["idea_id"] == "" or row["idea_id"] == "0":
                # Generate a new unique idea_id
                new_id = f"ID-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}-{i}"

                # Update the idea_id in the row
                row["idea_id"] = new_id
                idea_id_lst.append(new_id)

                # Add to new rows list
                new_rows.append(row)
        # Add new rows to the tracker
        if new_rows:
            new_rows_df = pd.DataFrame(new_rows)
            st.session_state["alpha_obj"].append_tracker(new_rows_df)
            st.success(f"Added {len(new_rows)} new row(s) with generated IDs.")

        # Save tracker and show affected rows
        if changed_rows.any() or new_rows:
            st.session_state["alpha_obj"].save_tracker()

            with st.expander("🔍 View Updated/Added Rows"):
                affected_rows = st.session_state["alpha_obj"].df[
                    st.session_state["alpha_obj"].df["idea_id"].isin(idea_id_lst)]
                st.dataframe(affected_rows)
        else:
            st.info("No changes detected.")

def main():
    st.set_page_config(layout="wide")
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Select a page:",
        ["Alpha Analysis", "Correlation Analysis"]
    )
    st.sidebar.markdown("---")
    if page == "Alpha Analysis":
        config = AlphaTrackerConfig()
        data_handler = AlphaDataHandler(config)
        ui = AlphaTrackerUI(config, data_handler)
        ui.sidebar()
        ui.main()
    elif page == "Correlation Analysis":
        st.write("This page will contain correlation analysis features.")

if __name__ == "__main__":
    main()

#
# st.set_page_config(layout="wide")
#
# # Set the path to your alpha tracker folder
# ALPHA_DIR = r"E:\OneDrive\DataStorage\iqc_alpha"  # Change this to your actual folder
#
# # --- SIDEBAR: File Selection and Loading ---
# st.sidebar.title("Alpha File Loader")
#
# # Get list of files in the directory
# files = [f for f in os.listdir(ALPHA_DIR) if f.endswith(".csv")]
#
# # Dropdown to choose a file
# selected_file = st.sidebar.selectbox("Select Alpha Tracker File", files)
#
# # Load button
# if st.sidebar.button("Load File"):
#     file_path = os.path.join(ALPHA_DIR, selected_file)
#     st.session_state["alpha_df"] = pd.read_csv(file_path)
#     st.session_state["file_loaded"] = selected_file
#     st.session_state["file_path"] = file_path
#
# # --- SIDEBAR: Filter Options ---
# st.sidebar.markdown("---")
# st.sidebar.subheader("View Filters")
#
# min_sharpe = st.sidebar.slider("Minimum Sharpe", min_value=-5.0, max_value=5.0, value=1.4, step=0.1)
# hide_reviewed = st.sidebar.checkbox("Hide Manually Reviewed", value=True)
#
# # --- MAIN PAGE ---
# st.title("Alpha Tagging Tool")
#
# if "file_loaded" in st.session_state:
#     st.success(f"Loaded: {st.session_state['file_loaded']}")
#     df = st.session_state["alpha_df"].copy()
#     edit_cols = ['link', 'passed_checks', 'failed', 'sharpe', 'fitness', 'turnover', 'weight_check', 'subsharpe', 'correlation', 'code',
#                  'manual_reviewed', 'submitted', 'note_1', 'note_2', 'idea_id']
#     all_cols = edit_cols + [col for col in df.columns if col not in edit_cols]
#     # Filter DataFrame based on sidebar inputs
#     view_df = df[all_cols].copy()
#
#     # Apply sidebar filters
#     view_df = view_df[view_df["sharpe"] >= min_sharpe]
#     if hide_reviewed:
#         view_df = view_df[view_df["manual_reviewed"] == False]  # Or whatever condition fits
#
#     view_df = view_df.sort_values(by='sharpe', ascending=False).reset_index(drop=True)
#
#     # Store original view for change tracking
#     st.session_state["original_view_df"] = view_df.copy()
#
#     edited_df = st.data_editor(view_df, num_rows="dynamic", use_container_width=True)
#
#     if st.button("Apply Changes to Full DataFrame"):
#         changed_df = edited_df.compare(st.session_state["original_view_df"], keep_equal=False)
#         changed_rows = changed_df.index.get_level_values(0).unique()
#         idea_id_lst = []
#         if not changed_rows.empty:
#             for row_idx in changed_rows:
#                 edited_row = edited_df.iloc[row_idx]
#                 idea_id = edited_row["idea_id"]
#                 idea_id_lst.append(idea_id)
#                 # Now use alpha_id to update full_df
#                 for col in all_cols:  # Only update editable columns
#                     st.session_state["alpha_df"].loc[st.session_state["alpha_df"]["idea_id"] == idea_id, col] = edited_row[col]
#
#             st.success(f"Updated {len(changed_rows)} row(s) in full_df.")
#             with st.expander("🔍 View Changed Rows"):
#                 st.dataframe(st.session_state["alpha_df"][st.session_state["alpha_df"]["idea_id"].isin(idea_id_lst)])
#         else:
#             st.info("No changes detected.")
#     if st.button("Save Changes"):
#         # Save the edited DataFrame back to the CSV file
#         st.session_state["alpha_df"].to_csv(st.session_state["file_path"], index=False)
#         st.success("Changes saved successfully!")
