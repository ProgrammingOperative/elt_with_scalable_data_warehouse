# traffic_data_etl

<!-- PROJECT LOGO -->
<br />
<p align="center">
  
  <h3 align="center">Traffic Data ELT</h3>

  <p align="center">
    ELT pipeline using PostgreSQL, Airflow, DBT, Redash and Superset.
    <br />
    <br />
    ·
    <a href="https://github.com/ProgrammingOperative/Migrate_traffic_data">Report Bug</a>
    ·
    <a href="https://github.com/ProgrammingOperative/Migrate_traffic_data/issues">Request Feature</a>
    .
  </p>
</p>



<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgements">Acknowledgements</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

The objective of this project was to migrate an ELT pipeline developed for the week 11 challenge using(MYSQL, DBT, Apache Airflow, and Redash) to a more scalable and robust ELT pipeline. This was accomplished by changing the two main components, namely the MySQL data warehouse to Postgres and the Redash dashboard to Superset.

### Built With

Tech Stack used in this project
* [PostgreSQL](https://www.postgresql.org/)
* [Apache Airflow](https://jquery.com)
* [dbt](https://laravel.com)
* [Redash](https://laravel.com)
* [Superset](https://superset.apache.org/)



<!-- GETTING STARTED -->
## Getting Started

  
### Installation

1. Clone the repo
   ```sh
   git clone https://github.com/ProgrammingOperative/traffic_data_etl
   ```

<!-- USAGE EXAMPLES -->
## Usage

### Adminer: 
Adminer (formerly phpMinAdmin) is a full-featured database management tool written in PHP. Used to access MYSQL and Postgres Databases.
- Postgres:
   ```sh
   Navigate to `http://localhost:8080/` on the browser
   use `postgres-dbt` server
   use `testdb` database
   use `dbtuser` for username
   use `pssd` for password
   ```
### Airflow: 
  Airflow is used for aurchestration and automation.
   ```sh
   Navigate to `http://localhost:8080/` on the browser
   use `admin` for username
   use `admin` for password
   ```
### DBT:
DBT is used for cleaning and transforming the data in the warehouses. 
- Airflow is used for automation of running and testing dbt models

### Redash
   ```sh
   open terminal and execute `docker-compose run — rm server create_db`
   using adminer create a user and grant read access
   Navigate to `http://localhost:5000/` on the browser
   ```
### Superset
- navigate to `localhost:8088` to access Airflow 


<!-- ROADMAP -->
## Roadmap

See the [open issues](https://github.com/ProgrammingOperative/Migrate_traffic_data/issues) for a list of proposed features (and known issues).



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE` for more information.



<!-- CONTACT -->
## Contact

Titus Wachira - wachura11t@@gmail.com

Project Link: [https://github.com/ProgrammingOperative/Migrate_traffic_data](https://github.com/ProgrammingOperative/Migrate_traffic_data)



<!-- ACKNOWLEDGEMENTS -->
## Acknowledgements
* [10 Academy](https://www.10academy.org/)



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[linkedin-url]: https://www.linkedin.com/in/titus-wachira-0ba20818b

