# Data Engineering
## MVP Submission
#### Matthew Zirpoli

### Pipeline
For this project, I am developing an NHL game outcome prediction application to be used by sports bettors.  The model uses Logistic Regression to produce predictions for each game based on the team's statistical features.  Three data sources are used to scrape features and inputs for the prediction model, including NaturalStatTrick, a popular site for advanced statistics, the offical NHL API for game results, and fivethirtyeight's ELO ratings for each teams ELO rating, a measure of their competitive strength. Each raw source is scraped for historical data (prior three seasons up to yesterday) and saved to its own table in SQL to initialize the database. New rows are added daily to each table from the prior day's games. A daily job in SQL merges the tables and calculates other columns to produce the full feature set. The model is then trained and predictions output to a SQL table for display.

### Front End
The front end of the application will display the games scheduled for the current day, along with the predicted outcome and probability in percentage. A card will appear for each game, which visually highlights the winner along with a small chart to display the probability both visually and numerically. This will be done via a Flask app that retrieves the prediction outputs and renders the page.

### Deployment
The application will be deployed to an Amazon EC2 instance and run within a Docker container. It is currently being developed in a virtual environment on an Ubuntu VM which is identical to the server it will be deployed to.  

### Progress
#### Done
- Script to initialize SQL database with scraped historical data
- Script to scrape and add rows to SQL for new data daily
- Script to merge tables and calculate features in SQL
- Script to retrieve features, train model, produce prediction results

#### Doing
- Flask app and front end to display predictions

#### To Do
- Script to orchestrate running daily script operations
- Build Dockerfile
- Deploy to EC2 instance
- Add datasource for Vegas odds for each game (Stretch)