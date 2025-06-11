select * from layoffs;
create table layoffs_staging like layoffs;
insert layoffs_staging select * from layoffs;
select * from layoffs_staging;
-- To elimate Duplicate, we are adding a new row called row_num:
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date` ,stage, funds_raised_millions) as row_num
from layoffs_staging;
-- Finding the duplicate:
with duplicate_cte as(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date` ,stage, funds_raised_millions) as row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num>1;

-- As we cant elimate the row in the existing table we are creating a new Dataset to elimate the duplicate:

create table `layoffs_staging2` (
`company` text,
`location` text,
`industry` text,
`total_laid_off` int default null,
`percentage_laid_off` text,
`date` text,
`stage` text,
`country` text,
`funds_raised_millions` int default null,
`row_num` int
);
-- Seeing the Table which we created --
select * 
from layoffs_staging2;
-- Inserting a values in the table--
insert into layoffs_staging2
select * ,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date` ,stage, funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num>1;

select *
from layoffs_staging2
where row_num>1;
-- successfully deleted the row with duplicated--

select *
from layoffs_staging2;

-- standardizing the data--
-- Finding issue in our data and solving it is called standardize--

-- 1) Total number of people laid off:
select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;
-- Answers is 1200 people got laid off and 1% of people got laid off

-- 2) Which company made 1% people laid off:
select *
from layoffs_staging2
where percentage_laid_off = 1;

-- which company maid highest number of laid off:
select * 
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;
-- Katetta company made the highest laid off which is 2400

-- 3) comapany raised highest fund:
select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

-- 4) Highest laid off company list:
select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

-- 5)Highest laid off industry list:
select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

-- 6) laid off year range:
select min(`date`), max(`date`)
from layoffs_staging2;

select * from layoffs_staging2;

-- 7) which country has made the highest amount of laid_off:
select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;
-- united state has made the highest laid_off

-- 8)Highest laod_off year:
select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 2 desc;
-- Ans: 2022 has the hghest laif_off

-- 9) 
select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;
-- ans:Post-IPO 

-- 10) Which month has made the highest laid_off:
select substring(`date`, 6, 2) as `Month`, sum(total_laid_off)
from layoffs_staging2
group by month
order by 1 desc;
-- Ans month has the highest laid_off

-- 11)
select substring(`date`, 1, 7) as `Month`, sum(total_laid_off)
from layoffs_staging2
group by month
order by 2 desc;

-- 12) 



