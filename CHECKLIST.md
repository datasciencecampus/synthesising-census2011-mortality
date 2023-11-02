# Quality assurance checklist

Quality assurance checklist from [the quality assurance of code for analysis and research guidance](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html).

_Completed by Michaela Lawrence, reviewed by Henry Wilde._

## Modular code

- [x] Individual pieces of logic are written as functions. Classes are used if more appropriate.
- [x] Code is grouped in themed files (modules) and is packaged for easier use.
- [x] Main analysis scripts import and run high level functions from the package.
- [x] Low level functions and classes carry out one specific task. As such, there is only one reason to change each function.
- [x] Repetition in the code is minimalised. For example, by moving reusable code into functions or classes.
- [x] Objects and functions are open for extension but closed for modification; functionality can be extended without modifying the source code.
- *N/A* Subclasses retain the functionality of their parent class while adding new functionality. Parent class objects can be replaced with instances of the subclass and still work as expected. 

Notes: Code is modular and written appropriately. Some functionality is not exposed to the user for the sake of making the code easier to use.
Regarding the final point, there are no user-defined classes so this point is not applicable.

## Good coding practices

- [x] Names used in the code are informative and concise.
- [x] Names used in the code are explicit, rather than implicit.
- [x] Code logic is clear and avoids unnecessary complexity.
- [x] Code follows a standard style, e.g. [PEP8 for Python](https://www.python.org/dev/peps/pep-0008/) and [Google](https://google.github.io/styleguide/Rguide.html) or [tidyverse](https://style.tidyverse.org/) for R.

Notes: Where code is original it follows PEP8 guidelines. Some source code is copied from the original creators so may appear to be slightly inconsistent.
Having said that, the whole code base is linted and formatted using `black`, `isort`, and `flake8`.

## Project structure

- [x] A clear, standard directory structure is used to separate input data, outputs, code and documentation.
- [x] Packages follow a standard structure.

Notes: Please see `STRUCTURE.md` for more information. 

## Code documentation

- [x] Comments are used to describe why code is written in a particular way, rather than describing what the code is doing.
- [x] Comments are kept up to date, so they do not confuse the reader.
- [x] Code is not commented out to adjust which lines of code run.
- [x] All functions and classes are documented to describe what they do, what inputs they take and what they return.
- [x] Python code is [documented using docstrings](https://www.python.org/dev/peps/pep-0257/). R code is [documented using `roxygen2` comments](https://cran.r-project.org/web/packages/roxygen2/vignettes/roxygen2.html).
- [x] Human-readable (preferably HTML) documentation is generated automatically from code documentation.
- [x] Documentation is hosted for easy access. [GitHub Pages](https://pages.github.com/) and [Read the Docs](https://readthedocs.org/) provide a free service for hosting documentation publicly.

Notes: Code is well-documented.

## Project documentation

- [x] A README file details the purpose of the project, basic installation instructions, and examples of usage.
- [x] Where appropriate, guidance for prospective contributors is available including a code of conduct.
- [x] If the code's users are not familiar with the code, desk instructions are provided to guide lead users through example use cases.
- [x] The extent of analytical quality assurance conducted on the project is clearly documented.
- [x] Assumptions in the analysis and their quality are documented next to the code that implements them. These are also made available to users.
- [x] Copyright and licenses are specified for both documentation and code. *Where code has been lifted this is made obvious.*
- *N/A* Instructions for how to cite the project are given. 
- [x] Releases of the project used for reports, publications, or other outputs are versioned using a standard pattern such as [semantic versioning](https://semver.org/).
- [ ] A summary of [changes to functionality are documented in a changelog](https://keepachangelog.com/en/1.0.0/) following releases. The changelog is available to users.
- *N/A* Example usage of packages and underlying functionality is documented for developers and users.
- *N/A* Design certificates confirm that the design is compliant with requirements.
- *N/A* If appropriate, the software is fully specified.

Notes: The project is documented appropriately considering all development takes place on DAP. There isn't a change log but it's not much of a priority. There isn't an example that users can walk through but everything that needs to be specified by a user is detailed, and a user guide for the data is available in `GUIDE.md`. Analytical quality assurance has been done by performing utility analysis on output data. 

## Version control

- [x] Code is [version controlled using Git](https://git-scm.com/).
- [x] Code is committed regularly, preferably when a discrete unit of work has been completed.
- [x] An appropriate branching strategy is defined and used throughout development.
- [x] Code is open-sourced. Any sensitive data are omitted or replaced with dummy data.
- [x] Committing standards are followed such as appropriate commit summary and message supplied.
- [x] Commits are tagged at significant stages. This is used to indicate the state of code for specific releases or model versions.
- *N/A* Continuous integration is applied through tools such as [GitHub Actions](https://github.com/features/actions), to ensure that each change is integrated into the workflow smoothly.

Notes: Version control has been used extensively throughout development. CI tools are not available in DAP.

## Configuration

- [x] Credentials and other secrets are not written in code but are configured as environment variables.
- [x] Configuration is written as code, and is clearly separated from code used for analysis.
- [x] The configuration used to generate particular outputs, releases and publications is recorded.
- *N/A* If appropriate, multiple configuration files are used and interchangeable depending on system/local/user.

Notes: Code does not contain secrets. 

## Data management

- *N/A for confidential data stored in DAP* All data for analysis are stored in an open format, so that specific software is not required to access them.
- *N/A* Input data are stored safely and are treated as read-only.
- *N/A* Input data are versioned. All changes to the data result in new versions being created, or [changes are recorded as new records](https://en.wikipedia.org/wiki/Slowly_changing_dimension).
- *N/A* All input data is documented in a data register, including where they come from and their importance to the analysis.
- [x] Outputs from your analysis are disposable and are regularly deleted and regenerated while analysis develops. Your analysis code is able to reproduce them at any time.
- *N/A* Non-sensitive data are made available to users. If data are sensitive, dummy data is made available so that the code can be run by others.
- *N/A* Data quality is monitored, as per [the government data quality framework](https://www.gov.uk/government/publications/the-government-data-quality-framework/the-government-data-quality-framework).
- [x] Fields within input and output datasets are documented in a data dictionary.
- [x] Large or complex data are stored in a database.
- [x] Data are documented in an information asset register. 

Notes: We are not responsible for the way that the input data is stored and managed. The outputted synthetic data will be documented in the information asset register.

## Peer review

- [x] Peer review is conducted and recorded near to the code. Merge or pull requests are used to document review, when relevant.
- [ ] Pair programming is used to review code and share knowledge. 
- *N/A* Users are encouraged to participate in peer review.

Notes: Due to a lack of resources, we could not use peer review as much as we would have liked. Also, pair programming was not possible in this project due to the lack of resource.

## Testing

Testing hasnt been as extensive as we might have liked. 

- [ ] Core functionality is unit tested as code. See [`pytest` for Python](https://docs.pytest.org/en/stable/) and [`testthat` for R](https://testthat.r-lib.org/). 
- [ ] Code based tests are run regularly.
- [ ] Bug fixes include implementing new unit tests to ensure that the same bug does not reoccur.
- [ ] Informal tests are recorded near to the code.
- *N/A* Stakeholder or user acceptance sign-offs are recorded near to the code.
- *N/A* Test are automatically run and recorded using continuous integration or git hooks.
- [x] The whole process is tested from start to finish using one or more realistic end-to-end tests. *This has been implimented by running privacy budgets being run though the whole pipeline*
- [ ] Test code is clean an readable. Tests make use of fixtures and parametrisation to reduce repetition.
- *N/A* Formal user acceptance testing is conducted and recorded.
- *N/A* Integration tests ensure that multiple units of code work together as expected.

Notes: Unit testing has not been implemented across the board. This is an area we might want to prioritise in the future.

## Dependency management

- [x] Required passwords, secrets and tokens are documented, but are stored outside of version control.
- [x] Required libraries and packages are documented, including their versions.
- *N/A* Working operating system environments are documented.
- *N/A* Example configuration files are provided.
- *N/A* Where appropriate, code runs independent of operating system (e.g. suitable management of file paths).
- *N/A* Dependencies are managed separately for users, developers, and testers.
- [x] There are as few dependencies as possible.
- *N/A* Package dependencies are managed using an environment manager such as [virtualenv for Python](https://virtualenv.pypa.io/en/latest/) or [renv for R](https://rstudio.github.io/renv/articles/renv.html).
- *N/A* Docker containers or virtual machine builds are available for the code execution environment and these are version controlled.

Notes: Not all of the dependency management points are relavant because the code is only designed to run on DAP which is a singular use development enviroment and Henry is the only developer. 

## Logging

- [x] Misuse or failure in the code produces informative error messages.
- [x] Code configuration is recorded when the code is run. 
- *N/A* Pipeline route is recorded if decisions are made in code.

Notes: Code configuration is recorded as part of the log file for that run (stored in `logs`).

## Project management

- [x] The roles and responsibilities of team members are clearly defined.
- [x] An issue tracker (e.g GitHub Project, Trello or Jira) is used to record development tasks.
- [x] New issues or tasks are guided by users’ needs and stories.
- *N/A* Issues templates are used to ensure proper logging of the title, description, labels and comments.
- [x] Acceptance criteria are noted for issues and tasks. Fulfilment of acceptance criteria is recorded.
- [x] Quality assurance standards and processes for the project are defined. These are based around [the quality assurance of code for analysis and research guidance document](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html).

Notes: Henry is the only developer so issue templates were not needed. Michaela and Mat have completed some reviewing. 