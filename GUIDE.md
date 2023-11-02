# Synthetic census-mortality dataset user guide

This guide serves as an accompaniment to a synthetic version of the linked
Census 2011 and deaths register datasets. We refer to this dataset as the 
synthetic census-mortality dataset. The purpose of this guide is to inform 
users of the dataset about its origins, contents, and quality.

The synthetic dataset and this user guide were created by the Synthetic Data
and Privacy-Enhancing Technologies Squad of the Data Science Campus in the
Office for National Statistics. The dataset was created using a method that
adheres to a formal definition of privacy called
[differential privacy](#what-is-differential-privacy), which has grown in 
popularity as a modern standard for disclosure control. This guide offers a
brief overview of topics like differential privacy in the [preliminaries and 
background section](#preliminaries).  

<div class="warning" style='background-color:#EDF; color: #637; border-left: solid #85D 4px; border-radius: 4px; padding:0.7em;'>
<span>
<p style='margin-top:1em; text-align:center'>
<b>Please bear the following things in mind when using this dataset</b></p>
<p style='margin-left:1em;'>

1. This is a **synthetic data product** and is
   **not suitable for decision-making**
2. We have included **several deliberate limitations** in the synthesis
   process, detailed below
3. We preserve all one-way and some two-way marginals, but
   **higher-order relationships** should be treated as **unreliable**

</p>
</p>
</span>
</div>

The remainder of this document is structured as follows:

0. [Preliminaries and background](#preliminaries)
1. [The synthesis method and its parameters](#the-synthesis-method)
2. [Decision points and deliberate limitations](#decisions-and-limitations)
3. [A glance at utility](#dataset-utility)
4. [A note on disclosure risk](#disclosure-risk)
5. [Data dictionary](#data-dictionary)


## Preliminaries

### What is synthetic data?

A synthetic dataset contains artificially generated data as opposed to genuine,
real-world data. Often synthetic datasets are made to resemble a real dataset
in some capacity. Typically, such datasets can be described as one of three
types: dummy, low-fidelity, or high-fidelity.

- **Dummy datasets** have the correct structure (i.e. column names and data
  types), but they do not resemble the real dataset in any meaningful way. This
  sort of data is often only used as a placeholder in testing pipelines.
- **Low-fidelity datasets** have the correct structure and each column
  preserves some important statistical characteristics of their real 
  counterpart. However,  they do not preserve any of the interactions between 
  columns. This sort of data carries a low disclosure risk but offers little 
  analytical value.
- **High-fidelity datasets** have the correct structure, and columns are
  preserved along with some interactions. The preserved relationships
  can be of any order, involving several columns at once. In general, this sort
  of data is the most accurate, but it also comes with higher disclosure risk.

The synthetic census-mortality dataset is a high-fidelity dataset. We discuss 
the [details of its creation](#the-synthesis-method) in the next section, 
including the relationships we preserve. We chose to create a high-fidelity 
dataset because it offers higher analytical quality than low-fidelity data. Our
objective in creating this synthetic dataset was to make something people would
want to use for plausible analysis, all while protecting the confidentiality of
the underlying data. We offer an overview of the datase's
[limitations](#decisions-and-limitations) below.

### What is differential privacy?

Differential privacy (DP) is a mathematical definition of privacy introduced in
[Dwork, McSherry, Nissim and Smith (2006)](https://doi.org/10.1007/11787006_1)
applied to mechanisms that act on confidential datasets. In the case of
differentially private synthetic data, the generation method must adhere to the
definition of DP, not the data itself. There are many variants of DP, and so
the family of definitions are sometimes referred to as "formally" private.

In essence, DP describes a bound on the risk to an individual's privacy if they
choose to contribute to a dataset that will be queried by a DP mechanism. This 
bound is described as a privacy loss budget and is primarily controlled by a 
parameter $\epsilon \ge 0$. Smaller values of $\epsilon$ imply stronger 
protection against privacy loss. A mechanism achieves DP by adding a controlled
amount of noise (set by the privacy budget) to results drawn from the data.

Interpreting $\epsilon$ can be difficult as it does not have a finite range. A 
rule of thumb [from academia](https://doi.org/10.1145/1866739.1866758) suggests 
an ideal range to be $0.01 < \epsilon < 1.1$, but much larger budgets are often 
used in practice.

For more details on differential privacy, its motivations, and its applications
we have some recommended reading:

- [This very approachable video](https://www.youtube.com/watch?v=pT19VwBAqKA)
  about the US Census Bureau's use of DP.
- [This report](https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/methodologies/applyingdifferentialprivacyprotectiontoonsmortalitydatapilotstudy)
  from ONS Methodology about why they decided not to use
  differential privacy for publishing marginal tables.
- The first two chapters of
  [this book](https://www.cis.upenn.edu/~aaroth/Papers/privacybook.pdf), 
  co-authored by Dwork, on the algorithmic foundations of differential privacy. 


## The synthesis method

We use the Maximum Spanning Tree (MST) method to synthesise the data. MST 
emerged as the winning method in the
[2018 NIST competition](https://www.nist.gov/ctl/pscr/open-innovation-prize-challenges/past-prize-challenges/2018-differential-privacy-synthetic)
on differentially private data synthesis. Since the competition's end, the 
authors of MST [generalised form](https://arxiv.org/pdf/2108.04978.pdf) of the 
method, which we adapted for our needs; the original method was highly tailored 
to the NIST competition dataset.

MST is a formally private synthesis method for synthesising categorical
datasets using low-dimensional marginals. A marginal is a effectively a table
of counts or a contingency table. A one-way marginal holds the frequency of
each unique value in a particular column of a dataset. A $k$-way marginal is a 
table of counts across $k$ columns in a dataset. For instance, the two-way 
marginal for sex by ethnicity in the census would be a table describing how
many people belonged to each sex-ethnicity combination.

This section provides an overview of the MST method, but
[this tutorial](https://daffidwilde.github.io/iao-mst-demo/) provides a closer
walkthrough using the 1% Census Teaching File as an example.

From a high level, MST consists of three stages:

1. **Select** a collection of low-dimensional marginals.
2. **Measure** those marginals by adding a controlled amount of noise to them.
3. **Generate** synthetic data to preserve those noisy marginals.

### Separating synthesis

The census-mortality dataset links the 2011 census to the deaths register, and
since there are far fewer deaths per year than citizens, it is unbalanced. In
fact, 90% of all the original records belong to individuals at the Census who
have not passed away. Hence, they contain no mortality data whatsoever.

This imbalance creates structural missing values in the data. So, to avoid
having to try and replicate them well, we synthesise census-only and mixed
records separately. During pre-processing, we identify and separate the dataset
into these parts. Then we apply MST as described below, and concatenate the
parts during post-processing.

### Select

Selecting marginals can be done by hand, although it requires significant input 
from domain experts. Alternatively, a selection algorithm can be used. For MST, 
this selection algorithm includes all one-way marginals, and a set of two-way
marginals selected using part of the privacy budget. The two-way marginals are 
selected to try and preserve the most informative, highly correlated pairs of
columns in the dataset.

The selection method measures each two-way marginal and calculates its "weight"
by comparing the true marginal to its noisy counterpart. Larger weights
indicate that the column pair is more informative, and thus more desirable. To 
ensure privacy, the pairs are randomly selected based on these weights such
that they form a maximum spanning tree linking all the dataset's columns. In 
practice, the resulting graph is not necessarily a tree but a directed acyclic
graph, and some sets of two-way marginals may form triangles.

<p align="center" width="100%">
  <img src="results/1.0/census_inclusion_heatmap.png" width="45%"/>
  &nbsp;
  <img src="results/1.0/mortal_inclusion_heatmap.png" width="45%">
</p>

The figures above show the selected marginals with $\epsilon = 1$ for each part
of the data: census-only records on the left and mixed records on the right.
Highlighted cells are included in the synthesis; green cells show (one- and
two-way) marginals included in the model once, while the dark blue cells
indicate a two-way marginal was included more than once. In each case, the
columns are ordered according to how they were synthesised; both begin with
``msoa_code``, while the census-only synthesis ends with ``sex``, and the mixed
synthesis ends with ``disability``.

### Measure

Measuring the selected marginals consumes the remainder of the privacy budget.
The budget is consumed by adding a controlled amount of Gaussian noise to the 
cells of each marginal table. This noise is defined according to the budget and
satisfies the definition of formal privacy used by MST. For an $\epsilon$ of 
one, each cell's count is perturbed (up or down) by about 16.

Accompanying each noisy marginal is some supporting data required for
generating the final synthetic dataset: the attributes in the marginal, a
weight for the attributes, and the amount of noise added to the marginal.
This weight is unrelated to the selection weight and, by default, each 
attribute set has equal weight.

### Generate

The generation step relies on a tool called
[Private PGM](https://arxiv.org/pdf/1901.09136.pdf) which was introduced by the
creators of MST. Private PGM infers a data distribution from a set of noisy 
measurements; its aim is to solve an optimisation problem to find a data 
distribution that would produce measurements close to the noisy observations.

Private PGM is highly flexible and provides acutely accurate synthetic data for
the selected marginals. It can also be used to accurately estimate marginals 
that weren't observed directly. However, it does come with some drawbacks when 
attempting to scale to the size and variety of the census-mortality dataset.


## Decisions and limitations

The MST method has been implemented in Python and made available on
[GitHub](https://https://github.com/ryan112358/private-pgm). This
implementation is effective and robust, but relies on the standard open-source
Python data science stack: [NumPy](https://numpy.org/) and
[Pandas](https://pandas.pydata.org/). These packages are limited in scale to a
single machine and (natively) whatever can fit onto a single core. As such, we
had to make some adjustments when applying it to the census-mortality dataset.

The code used to synthesise the census-mortality dataset relies on the 
implementation of MST wherever possible, but it is written to work on a cluster 
via PySpark. Reimplementing the Private PGM method in PySpark was considered 
too large a task, and so several decisions were made to expedite the synthesis. 
This section details those key decisions and their implications.  

### Disallowing column-sets with large domains

One of the limitations in scaling MST comes from the size of the dataset
domain. A dataset domain records the number of unique values in each column,
and its size is the product of all these counts.

We handle the issue of creating a model with too large a domain in the
selection process. As well as the usual conditions from MST, we only sample
pairs if their inclusion would not create a domain beyond what can comfortably
fit in memory. We have found this limit to be 2GB for the census-mortality
dataset.

### Omitting some marginals

In addition to the limit on the size of the overall domain, there are also
computational limits on the size of any individual marginal. The implementation
of MST can only work on marginal tables that fit into memory. For our purposes,
we set a limit at one million cells which filters out potential pairs in the
selection process.

As a result, some typically important relationships are not included in the
synthesis. For instance, we do not preserve the relationship between underlying
cause of death ICD code (``fic10und``) and the final mention death code
(``fimdth10``). These omissions will likely result in some confusing,
unrealistic column combinations, reducing utility.

### Synthesising in batches

The original dataset is large, containing tens of millions of rows. To leverage
the existing MST implementation within the constraints of DAP, we must break
our synthesis into parts. The synthetic dataset contains 50 million records and
is made from 20 chunks of 2.5 million rows. The main drawback of this is that
the dependence structure of the entire synthetic dataset is not identical to
one made as a whole. Chunking in this way has no bearing on the privacy of the
dataset, but it does affect the quality.

We chose the maximum chunk size possible to get a large dataset. Smaller chunk
sizes (in the hundreds of thousands of rows) led to exceptionally poor utility.

### MSOAs as the lowest geography

We synthesise the data with MSOA as the lowest level of geography for two
reasons. First, using LSOAs (the lowest in the original data) leads to a number
of marginals too large for MST - as described above. Second, it provides some
additional obfuscation around the confidentiality of any individual row.

To avoid unrealistic, commonly found issues, we do not synthesise higher
geographies directly. Instead, we fill in local authorities and regions based
on the synthesised MSOA code during post-processing.

### No household hierarchies

MST synthesises categorical data, and so household identifiers would need to be
treated as such. Given the number of households in the census, categorising the
identifiers would be untenable. There do exist post-processing methods, such as
[SPENSER](https://lida.leeds.ac.uk/research-projects/spenser-synthetic-population-estimation-and-scenario-projection-model/),
for creating realistic households from individual-level synthetic data, but
incorporating them is currently beyond the scope of our project.

### No comorbidity

We only synthesise the leading final mention code (``fic10men1``) while the
original dataset includes a number of columns for concurrent conditions. Since
each of these columns can be any ICD-10 code, their domain lies in the
thousands, and are too large for MST to handle.

Moreover, synthesising realistic combinations of comorbidities is beyond the
scope of MST. The potential to synthesise these columns in a differentially
private way is a matter of future work.


## Dataset utility

The quality of a synthetic dataset is referred to as its utility. Typically,
a synthetic dataset's utility describes how well it represents the original
data and how useful it is. Determining what it means "to represent a dataset"
or "be useful" is the topic of much debate, and there are many ways of peeling
away the layers of that onion.

Broadly, you can split utility measures into one of two types: generic and
specific. Generic utility measures capture how well the synthetic dataset
preserves general, usually low-order, statistical properties of the real data.
Meanwhile, specific utility measures describe how well the synthetic data
performs at a given task compared to the real data. For instance, you might
repeat a piece of analysis with the synthetic data and compare the results with
those from the original data.

When creating the synthetic census-mortality data we made use of a suite of
generic utility measures and one specific utility measure.

### Generic utility

The generic utility suite measures three aspects:

1. How well each column is preserved individually
2. How well the pairwise trends (i.e. correlations) were preserved
3. How easily the synthetic data could be distinguished from the real data.

The figures below show the coverage and similarity of each column individually
when $\epsilon = 1$. In each case how you measure coverage or similarity
depends on the data type of the column; despite synthesising all columns as
categorical, we measure their utility in their intended form. You can see that
the vast majority of columns cover the range of possible values well, and that
they resemble the original columns.

<p align="center", width="100%">
  <img src="results/1.0/coverage.png" width="45%"/>
  &nbsp;
  <img src="results/1.0/similarity.png" width="45%"/>
</p>

Similarly, the pairwise trends (correlation and contingency table comparisons)
are exceptionally well-preserved across the board as shown in the figure below.

The figure shows a comparison of pairwise trends (correlations and contingency
table agreement) for all pairs of columns in the data.

<p align="center" width="100%">
  <img src="results/1.0/pairwise.png" width="80%"/>
</p>

One of the key benefits of MST is that it can preserve low-level interactions
even if they weren't included in the selection process. In the figure above,
selected two-way cliques have been highlighted with a white border; there are
many pairs that show high quality despite only being synthesised indirectly.

The final arm to our utility analysis measures the distinguishability of the
synthetic data, and the results can be seen in the box plot below.
Distinguishability indicates how well the synthetic data preserves the entire
joint distribution of the original data. To measure distinguishability, we use
the [SPECKS](https://doi.org/10.1007/s40300-021-00201-0) method with an
off-the-shelf classification algorithm (a random forest).

<p align="center" width="100%">
  <img src="results/1.0/specks.png" width="80%">
</p>

The SPECKS metric takes values between 0 and 1 to measure distinguishability.
We plotted ``1 - SPECKS`` above so that a score of 1 indicates that the
synthetic data is completely indistinguishable from the original data. As can
be seen, our synthetic data performs very well in this regard.

### Specific utility

The specific utility metric emulates some ONS analysis on
[ethnic differences in life expectancy](https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/lifeexpectancies/articles/ethnicdifferencesinlifeexpectancyandmortalityfromselectedcausesinenglandandwales/2011to2014#linking-death-registrations-to-the-2011-census).
In the report, the authors created a table of life expectancies by sex and
ethnic group, including the marginal "gaps" in those estimates. These gaps are
the range of the associated column or row.

We recreated the analysis to calculate life expectancy by sex and ethnicity
using the real and synthetic datasets. We measure specific utility by looking
at the relative difference between each estimate and gap. The figure below
shows the results for $\epsilon = 1$.

<p align="center" width="100%">
  <img src="results/1.0/life_expectancy_relative_change.png" width="80%"/>
</p>

It looks like individual life expectancies are preserved reasonably well.
However, neither the gaps in ethnicity or sex survived the synthesis. While
this may appear to be a flaw in the synthetic data, why should we expect
anything else? The MST method as implemented here is generic and relies on a
selection of low-level interactions. If high specific utility is required,
synthesising for that purpose is highly advisable, but the aim for this dataset
was to create something reasonably usable in a range of settings.


## Disclosure risk

The formal guarantee of differential privacy also offers some practical insight
into the disclosure risk of the MST method. As a reminder, MST measures a
collection of low-dimension marginals by adding controlled noise to them. For a
given budget, we can calculate the amount of noise added to each cell in a
marginal table. When $\epsilon = 1$, MST adds (or subtracts) approximately 60
to each real cell count before moving to the generation step.

Moreover, MST includes a "compression" process as a preliminary part of the
measurement step. This process identifies rarely occurring categories in each
noisy column and gathers them into a single category. The real data is then
recoded using this "compressed" domain set of values. The privacy budget
determines how rare categories have to be to be grouped together. For
$\epsilon = 1$, the minimum threshold of any noisy cell count is approximately
180, which is well above the standard disclosure risk threshold of five.

Although we have not conducted a formal privacy analysis of the synthetic
census-mortality dataset, we do intend on carrying out a number of privacy
attacks on the data and synthesis method. This analysis will likely include
traditional statistical disclosure control tests as well as more sophisticated,
targeted attacks.


## Data dictionary

This section provides an overview of the columns in the data, describing:

- **Name**: the name of the column
- **Description**: a brief description of the column
- **Origin**: which dataset the column belonged to originally (census or deaths
  register)
- **Domain**: how many unique values exist in the original data, including
  missing values
- **Type**: the `pyspark` data type of the column
- **Value**: the range or a description of the possible values
- **Inclusion**: how the column became part of the synthetic data (direct
  synthesis, engineered or attached)

| Name              | Description                                    | Origin | Domain | Type    | Value                             | Inclusion   |
|:------------------|:-----------------------------------------------|:------:|-------:|:-------:|:---------------------------------:|:-----------:|
| `age_census`      | Age at time of Census                          | Census |    116 | String  | 3-digit string                    | Engineered  |
| `age_death`       | Age at time of death                           | Deaths |    126 | Integer | `[0...124,NA]`                    | Engineered  |
| `ceststay`        | Duration of stay in communal establishment     | Deaths |      5 | String  | `[%,&,1,2,NA]`                    | Synthesised |
| `cob`             | Country of birth code                          | Census |    272 | String  | 3-digit code                      | Synthesised |
| `deprived`        | Classification of household deprivation        | Census |      7 | String  | `[1...5,X]`                       | Synthesised |
| `disability`      | Long-term health problem or disability         | Census |      4 | String  | `[1,2,3,X]`                       | Synthesised |
| `dob_day`         | Date of birth day                              | Census |     31 | String  | `DD`                              | Synthesised |
| `dob_month`       | Date of birth month                            | Census |     12 | String  | `MM`                              | Synthesised |
| `dob_year`        | Date of birth year                             | Census |    116 | String  | `YYYY`                            | Synthesised |
| `doddy`           | Date of death day                              | Deaths |     32 | Integer | `[1...31,NA]`                     | Synthesised |
| `dodmt`           | Date of death month                            | Deaths |     13 | Integer | `[1...12,NA]`                     | Synthesised |
| `dodyr`           | Date of death year                             | Deaths |     13 | Integer | `YYYY` or `NA`                    | Synthesised |
| `ecocatpuk11`     | Economic category                              | Census |      9 | String  | `[1...8,X]`                       | Synthesised |
| `estnature`       | Establishment nature                           | Census |     26 | String  | `[00....23,WW,YY]`                | Synthesised |
| `ethpuk11`        | Ethnic group                                   | Census |     19 | String  | `[01...18,XX]`                    | Synthesised |
| `fic10men1`       | First final mention ICD10 code                 | Deaths |   2943 | String  | ICD10 code or `NA`                | Synthesised |
| `fic10und`        | Final underlying cause of death ICD10 code     | Deaths |   4512 | String  | ICD10 code or `NA`                | Synthesised |
| `fimdth10`        | Final manner of death code                     | Deaths |     62 | String  | 3-digit code or `NA`              | Synthesised |
| `fmspuk11`        | Family status                                  | Census |     10 | String  | `[1...9,X]`                       | Synthesised |
| `health`          | General health                                 | Census |      6 | String  | `[1...5,X]`                       | Synthesised |
| `hhchuk11`        | Household composition                          | Census |     28 | String  | `[01...26,XX,NA]`                 | Synthesised |
| `hlqpuk11`        | Highest level of qualification                 | Census |      8 | String  | `[10...16,XX,NA]`                 | Synthesised |
| `indgpuk11`       | Industry                                       | Census |     22 | String  | `[01...21,XX]`                    | Synthesised |
| `la_code`         | Local Authority at time of Census              | Census |    348 | String  | 11-digit code or `NA`             | Attached    |
| `langprf`         | English proficiency                            | Census |      5 | String  | `[1...4,X]`                       | Synthesised |
| `laua`            | Local Authority of place of residence at death | Deaths |    360 | String  | 11-digit code or `NA`             | Synthesised |
| `marstat`         | Marital and civil partnership status           | Census |      9 | String  | `[1...9]`                         | Synthesised |
| `msoa_code`       | MSOA of residence at time of Census            | Census |   7201 | String  | 11-digit code                     | Synthesised |
| `occ`             | Occupation                                     | Census |    370 | String  | 4-digit code or `XXXX`            | Synthesised |
| `occpuk111`       | Occupation (minor group)                       | Census |     91 | String  | 3-digit code or `XXX`             | Attached    |
| `occpuk113`       | Occupation (major group)                       | Census |     10 | String  | `[1...9,X]`                       | Attached    |
| `position`        | Position in communal establishment             | Census |      4 | String  | `[1...3,NA]`                      | Synthesised |
| `postmort`        | Post-mortem indicator                          | Deaths |      4 | String  | `[1...3,NA]`                      | Synthesised |
| `ppbroomhew11`    | Number of persons per bedroom in household     | Census |      6 | String  | `[1...4,X,NA]`                    | Synthesised |
| `region_code`     | Region of residence at time of Census          | Census |     10 | String  | 9-digit code                      | Attached    |
| `relpuk11`        | Religion                                       | Census |     10 | String  | `[1...9,X]`                       | Synthesised |
| `residence_type`  | Residence type                                 | Census |      2 | String  | `[C,H]`                           | Synthesised |
| `rgn`             | Region of place of residence at death          | Deaths |     13 | String  | 9-digit code or `NA`              | Attached    |
| `ruralurban_code` | Urban/rural code of household                  | Census |     10 | String  | `[A1,A2,C1,C2,D1,D2,E1,E2,F1,F2]` | Synthesised |
| `sex`             | Sex of respondent                              | Census |      2 | String  | `[1,2]`                           | Synthesised |
| `tenhuk11`        | Tenure of household                            | Census |     12 | String  | `[0...9,X,NA]`                    | Synthesised |
| `transport`       | Method of transport to work                    | Census |     12 | String  | `[01...11,XX]`                    | Synthesised |
| `uresindpuk11`    | Usual resident indicator                       | Census |      2 | String  | `[0,1]`                           | Synthesised |
| `yrarrpuk11`      | Year of arrival in UK (binned)                 | Census |     13 | String  | `[01...12,XX]`                    | Synthesised |
