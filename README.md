# Predicting Risk of Worsening in IBD Patients using NLP on Reddit

## Justification
Patients often share symptoms and early warning signs of worsening conditions in forums and social media. A model capable of recognizing language associated with worsening may serve as a population surveillance tool and generate clinical hypotheses.

## Research Question
Can we predict, with useful accuracy, the risk of worsening or relapse in IBD patients based on the language they use on Reddit?

## General Objective
Develop an NLP pipeline to identify posts indicative of worsening/relapse in users discussing IBD on Reddit and Twitter, and evaluate its performance and main linguistic signals.

## Specific Objectives
1. Collect and build a corpus of posts/tweets in English and/or Spanish related to IBD.
2. Annotate a seed dataset with labels: worsening vs not-worsening.
3. Train classification models (baseline: TF-IDF + logistic/RF; advanced: fine-tuned BERT/BioBERT) to detect worsening language.
4. Evaluate models (AUC, F1, precision/recall) and explain predictions (SHAP / LIME / important tokens).
5. Compare differences between CD and UC (subreddit / keywords) and generate interpretable visualizations.

---

## 2) Methodology

### Phase 0 — Definition of signals
- Define phrases that indicate worsening: “flare”, “hospitalized”, “started steroids”, “stool with blood”, “worsening pain”, “couldn’t eat” etc.
- Create initial keyword list for scraping (crowdsourced + literature review).

### Phase 1 — Data Collection
- **Reddit**: use `praw` or `psaw` to extract posts + comments from subreddits:  
  r/IBD, r/CrohnsDisease, r/UlcerativeColitis, r/ChronicIllness  


### Phase 2 — Preprocessing
- Normalization (lowercase etc), remove URLs, optional emoji removal, expand contractions, clean unnecessary symbols.
- Tokenization, lemmatization with spaCy (English model) or stanza.
- Explicit language detection.

### Phase 3 — Annotation
- Create short annotation guide.
- Minimum labels: flare (1) vs no-flare (0) vs ambiguous (2).
- Each post annotated by 2 people; resolve disagreements via adjudication.
- Minimum target size for prototype: 500–1000 examples (more improves performance).

### Phase 4 — Modeling

**Baselines**
- TF-IDF + Logistic Regression (regularized)
- TF-IDF + Random Forest / XGBoost

**Advanced**
- Fine-tune `distilbert-base-uncased` or `bert-base-uncased` (or BioBERT / PubMedBert biomedical vocabulary) for ternary/binary classification.
- Training with `transformers` (HuggingFace) + `Trainer` (early stopping, class weights).

**Inputs**
- Full text, optionally with extra features: symptom counts, presence of keywords, metadata (upvotes, length).

### Phase 5 — Validation and Evaluation
- Train/val/test stratified split (by user?).  
- Metrics: ROC-AUC, Precision-Recall AUC (useful if classes are imbalanced), F1 (macro/weighted), precision@ if detecting top-suspects matters.  
- Calibration curve if useful probabilities are needed.  

### Phase 6 — Interpretation
- SHAP on TF-IDF or LIME/Integrated Gradients for BERT: identify tokens/patterns contributing most to flare prediction.  
- Extract representative phrases per class.  

### Phase 7 — Visualization and Reporting
- Word clouds, token heatmaps, timeline of tweets/posts with flares, CD vs UC comparison.  
- Prepare notebook + README + small web demo (optional: Streamlit).  

---

## 3) Repo Structure

Top level

	•	README.md — project overview, setup, how to run scraping and notebooks.
	•	requirements.txt — dependencies.
	•	.gitignore — ignore data/raw, output, .env, .venv, checkpoints.
	
Source code
	•	src/ibd_reddit/
	•	init.py
	•	extract.py — PRAW scraping utilities: scrape_subreddit, rate-limit handling, pagination.
	•	pushshift.py — streaming .zst NDJSON readers and filters.
	•	parse.py — text cleaning, field normalization, timestamps, subreddit filters.
	•	io.py — path helpers, save/load CSV/Parquet to output/ and data/.
	•	config.py — .env loading, constants (subreddit lists), runtime settings.
	•	logging.py (optional) — simple, consistent logging formatters.
	
Scripts (CLI entrypoints)
	•	scripts/
	•	reddit_scrapper.py — fetch last 1000 posts per subreddit → output/{subreddit}_lastN_YYYYMMDD.csv.
	•	reddit_extractor.py — batch/cron jobs (e.g., daily incremental runs) built on src functions.
	•	test_connection.py — quick check for PRAW credentials and API reachability.
	
	
Notebooks (main analysis lives here)
	•	notebooks/
	•	01_eda_ibd.ipynb — initial exploration of posts/comments.
	•	02_clean_features.ipynb — cleaning, feature engineering, labels.
	•	03_modeling_baseline.ipynb — baseline models/visuals.
	•	99_scratchpad.ipynb — temporary experiments (can be git-ignored if preferred).

Data lifecycle
	•	data/
	•	raw/ — immutable sources (original .zst dumps, raw API exports).
	•	interim/ — temporary cleaned/intermediate tables.
	•	processed/ — curated datasets ready for modeling/EDA.
	
Outputs and reports
	•	output/
	•	figures/ — exported plots for docs/reports.
	•	reports/ — HTML/Markdown/PDF summaries if generated.	
	

---

## 4) Deliverables
- Anonymized dataset (CSV) with text, label, source, date.  
- Reproducible scripts: scraping, preprocess, train, evaluate.  
- Notebook with exploratory analysis and results.  
- Trained model (weights) + explanation (SHAP plots).  
- Paper  
- README + project documentation.  
- Presentation with findings and limitations.  

---

## 5) Ethics and Limitations
- **Privacy**: only use public content. Anonymize IDs; do not publish usernames.  
- **Consent**: Reddit/Twitter are public, but treat data respectfully; consider removing or shortening direct quotes.  
- **Bias / Representativeness**: social media users do not represent all populations → limitation.  
- **Clinical validation**: the model suggests signals, does not replace diagnosis; clarify.  

---

## 7) Success Metrics
- Baseline TF-IDF + LR: **F1 ≥ 0.65 on test**  
- Fine-tuned BERT: **+0.05–0.10 F1 improvement over baseline**  
- Interpretability: list of relevant tokens/phrases and 10 correctly identified posts as examples  

---

## 8) Libraries
- **Scraping**: praw, psaw, snscrape  
- **Preprocessing**: spaCy, nltk, emoji, ftfy  
- **Modeling**: scikit-learn, xgboost, transformers (HuggingFace), torch  
- **Interpretation**: shap, lime, captum (for pytorch)  
- **Visualization**: matplotlib, seaborn, plotly, wordcloud  
- **Reproducibility**: requirements.txt, Makefile or poetry, notebooks  
