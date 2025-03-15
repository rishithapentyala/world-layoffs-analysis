-- EDA on cleaned data

select * from layoffs_staging2;

-- Maximum nuber of employees removed in 1 day from the entire dataset.
select company,`date`,total_laid_off from layoffs_staging2 where total_laid_off=(select max(total_laid_off) from layoffs_staging2);

-- all the companies that completely shut down
select count(*) from layoffs_staging2 where percentage_laid_off=1;
select * from layoffs_staging2 where percentage_laid_off=1 order by total_laid_off DESC;

-- all the companies that completely shut down which had huge funding
select * from layoffs_staging2 where percentage_laid_off=1 order by funds_raised_millions DESC;

-- company wise total laid off over the entire dataset
select company,sum(total_laid_off) from layoffs_staging2 group by company  order by sum(total_laid_off)DESC;

-- which industry got effected the most,industry with maximum laidoffs
select industry,sum(total_laid_off) from layoffs_staging2 group by industry  order by sum(total_laid_off)DESC;

-- which country had the most laidoffs
select country,sum(total_laid_off) from layoffs_staging2 group by 1 order by 2 DESC;

-- year wise max laid offs
select year(`date`) ,sum(total_laid_off) from layoffs_staging2 group by 1 order by 2 DESC;

-- stage wise max Laid offs
select stage ,sum(total_laid_off) from layoffs_staging2 group by 1 order by 2 DESC;

-- progression of layoffs(rolling sum)


select substring(`date`,1,7) as `Month`,sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `Month`
order by 1 ASC;

-- month wise rolling sum of laid offs
with rolling_total as
(select substring(`date`,1,7) as `Month`,sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `Month`
order by 1 ASC)
select `Month`,total_off,sum(total_off) over(order by `Month`) as rolling_total
from rolling_total ;

-- year wise layoffs per company
with company_rolling_total as
(select company ,year(`date`) as `year`,sum(total_laid_off) as total_off
from layoffs_staging2
where year(`date`)  is not null
group by company,year(`date`)
order by 1 ASC)
select company,`year`,total_off,sum(total_off) over(partition by company order by `year`) as rolling_total
from company_rolling_total;


-- ranking companies with most layoffs year wise(top 5)
select company ,year(`date`) as `year`,sum(total_laid_off) as total_off
from layoffs_staging2
group by company,`year`
order by 3 desc;

with company_year(company,`year`,total_laid_off) as
(select company ,year(`date`) as `year`,sum(total_laid_off)
from layoffs_staging2
group by company,`year`),
company_rank as 
(select *,dense_rank()over(partition by `year`order by total_laid_off desc) as Ranking from company_year where `year` is not null)
select * from company_rank where Ranking <=5;

-- ranking industries with most layoffs year wise
with industry_year(industry,`year`,total_laid_off) as
(select industry ,year(`date`) as `year`,sum(total_laid_off)
from layoffs_staging2
group by industry,`year`),
industry_rank as 
(select *,dense_rank()over(partition by `year`order by total_laid_off desc) as Ranking from industry_year where `year` is not null)
select * from industry_rank order by Ranking;