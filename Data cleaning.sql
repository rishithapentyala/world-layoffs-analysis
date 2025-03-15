SELECT * FROM layoffs;

-- 1.remove duplicates
-- 2.standardize data
-- 3.null or blank values
-- 4.remove columns and rows

-- creating a duplicate table to work on
create table layoffs_staging like layoffs;

select * from layoffs_staging;

insert layoffs_staging
select * from layoffs;
-- identify duplicates
select *,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

with duplicates_cte as(
select *,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging)
select * from duplicates_cte 
where row_num>1;

select * from layoffs_staging 
where company='cazoo';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

select * from layoffs_staging2
where row_num>1;
delete from layoffs_staging2
where row_num>1;
select * from layoffs_staging2;

-- standardizing data
select * from layoffs_staging2;

select company,(trim(company))
from layoffs_staging2;

update layoffs_staging2 set company=trim(company);

select distinct(industry) from  layoffs_staging2
order by industry;

-- grouping crypto

select * from layoffs_staging2 
where industry like 'Crypto%';

update layoffs_staging2 set industry='Crypto'
where industry like 'Crypto%';

select distinct(country) from layoffs_staging2
order by country;
 
select distinct(country),trim(trailing'.' from country) from layoffs_staging2 order by country;

update layoffs_staging2 set country='United States'
where country like 'United States%';

select `date`,str_to_date(`date`,'%m/%d/%Y') from layoffs_staging2;

update layoffs_staging2 set `date`=str_to_date(`date`,'%m/%d/%Y') ;
select `date` from layoffs_staging2;

alter table layoffs_staging2
modify column `date` DATE;

-- null value handling
select * from layoffs_staging2
where industry is NULL or industry='';

select * from layoffs_staging2 where company='Airbnb';
select * from layoffs_staging2 where company like 'Bally%';
select * from layoffs_staging2 where company ='Carvana';
select * from layoffs_staging2 where company ='Juul';

select* from layoffs_staging2 t1 join layoffs_staging2 t2
on t1.company=t2.company
and t1.location =t2.location
where (t1.industry is NULL or t1.industry='' )AND t2.industry is not NULL;




update layoffs_staging2 set
industry=NULL where industry='';
update layoffs_staging2 t1 
join layoffs_staging2 t2 on t1.company=t2.company 
set t1.industry=t2.industry
where (t1.industry is NULL)AND t2.industry is not NULL;

select * from  layoffs_staging2 where total_laid_off is NUll and percentage_laid_off is null;
delete
from layoffs_staging2 where total_laid_off is NUll and percentage_laid_off is null;

select * from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;



