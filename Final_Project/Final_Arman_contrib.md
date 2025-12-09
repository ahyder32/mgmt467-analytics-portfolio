# Final_Arman_contrib.md

## Project & Role

**Project:** Earthquake Risk Analytics (MGMT 467 – Big Data Analytics in the Cloud)  
**Name:** Arman Hyder  
**Notebook:** `Final_Arman_analysis.ipynb`  

In this project I focused primarily on **batch data preparation, depth prediction modeling in BigQuery ML, and analytical visualization** that informed our dashboard and overall interpretation of the earthquake dataset.

---

## Technical Tasks Completed

### 1. Batch Data Preparation & Curation

- Imported the Kaggle earthquake dataset into **BigQuery** and worked with the team dataset `mgmt-467-47888-471119.earthquake`.
- Designed and implemented the curated table **`earthquake_bqml_data`**, including:
  - Selecting the core modeling features: `Latitude`, `Longitude`, `Depth`, `Type`, `Magnitude Type`, `Year`, `Month`, `Day`, `Hour`, and `Magnitude`.
  - Ensuring consistent types and cleaning invalid values (e.g., filtering out missing or nonsensical depths/magnitudes).
- Implemented a **data quality check** in BigQuery on the raw `earthquake.database` table:
  - Counted missing magnitudes and depths.
  - Flagged out-of-range magnitudes and invalid latitude/longitude coordinates.
  - Used this to justify the cleaning and filtering choices in the curated table.

### 2. BQML Depth Regression Model

- Designed and trained the linear regression model  
  **`mgmt-467-47888-471119.earthquake.earthquake_depth_model`** using **BigQuery ML**:
  - Target (label): `Depth`
  - Features: `Latitude`, `Longitude`, `Magnitude`, `Type`, `Magnitude Type`, `Year`, `Month`, `Day`, and `Hour`.
- Updated the original model cell (which predicted Magnitude) to correctly use the **preprocessed batch table** and treat **Depth as the continuous label**, aligning the model with the business question.
- Evaluated the model via `ML.EVALUATE`, inspecting metrics such as **mean_squared_error**, **mean_absolute_error**, and **r2_score**, and used these results to comment on how well depth can be explained by location, magnitude, and timing features.

### 3. Model Explainability (ML.EXPLAIN_PREDICT)

- Implemented an **`ML.EXPLAIN_PREDICT`** query on a sample of rows from `earthquake_bqml_data`:
  - Retrieved top feature contributions per prediction.
  - Verified that intuitive drivers like **magnitude** and **geographic coordinates** were influential in depth predictions.
- Used these explanations to discuss why certain earthquakes are predicted as shallow vs deeper and how that might matter for downstream risk interpretation.

### 4. Interactive Plotly Visualization

- Created an **interactive Plotly scatter** in the notebook that visualizes:
  - **Actual depth** vs **predicted depth** from `earthquake_depth_model`.
  - Points colored by **Magnitude**, with hover information including latitude, longitude, date/time, and magnitude type.
- Added a diagonal “perfect prediction” reference line to visually assess model accuracy:
  - Points close to the line indicate good predictions, while large deviations highlight where the model struggles.
- This figure directly informed the design of the **model performance / insight section** of our Looker Studio dashboard (e.g., how well the model captures shallow vs deep events).

### 5. Prompt Engineering & DIVE Entries

- Documented prompt iterations and modeling decisions in the **DIVE-style journal cells** inside `Final_Arman_analysis.ipynb`, focusing on:
  - The pivot from predicting magnitude to predicting depth (and why depth was more aligned with our analysis goals).
  - Correct handling of labels and passthrough columns when using `ML.PREDICT` (e.g., keeping `actual_depth` separate from features).
  - Validation steps comparing actual vs predicted depth and interpreting the results through both metrics and visualizations.

---

## Pull Requests and Code Contributions

> Note: Replace the placeholders below with your actual repo + PR links.

- [PR #X – Create `earthquake_bqml_data` curated table and cleaning logic](https://github.com/<org>/<repo>/pull/XX)  
  Defined the schema, filtering conditions, and transformation from `earthquake.database` to the modeling-ready table.
- [PR #Y – Add `earthquake_depth_model` and evaluation queries](https://github.com/<org>/<repo>/pull/YY)  
  Implemented BQML `LINEAR_REG` model for Depth, plus `ML.EVALUATE` and `ML.EXPLAIN_PREDICT` SQL.
- [PR #Z – Notebook analysis and Plotly visualization](https://github.com/<org>/<repo>/pull/ZZ)  
  Added the `Final_Arman_analysis.ipynb` notebook section for actual vs predicted depth, including the interactive Plotly figure and DIVE documentation.

---

## Lessons Learned

- **Schema design matters for ML**: Taking the time to create a clean, modeling-ready table (`earthquake_bqml_data`) made the BQML workflow much smoother and reduced debugging around types and missing values.
- **Label choice is critical**: Initially targeting the wrong label (Magnitude) showed that even a perfectly working pipeline can answer the wrong question. Switching to **Depth** aligned the model with a more meaningful analytical goal.
- **Explainability builds trust**: Using `ML.EXPLAIN_PREDICT` gave insight into which features the model relied on most, which is essential when sharing results with non-technical stakeholders who care about why a prediction was made.
- **Visualization completes the story**: The interactive Plotly chart was more than just “eye candy”; it highlighted where the model performed well or poorly and directly influenced how we presented model performance in the dashboard and final presentation.
