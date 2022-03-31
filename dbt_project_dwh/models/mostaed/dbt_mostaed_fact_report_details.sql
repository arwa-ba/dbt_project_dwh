with cte AS (
SELECT e.QuestionUniqueId,
       e.QuestionNameAr,
       e.QuestionScore,
       e.EventId,
       e.SubmitionDate,
       e.Location,
       e.TenantId,
       value questionAnswers,
	   e.row_inserted_at
FROM {{source ('mostaed', 'Eventanswers') }}  e WITH (NOLOCK)
    CROSS APPLY STRING_SPLIT(QuestionAnswers, ',')
    LEFT JOIN {{source ('mostaed', 'Event') }}  AS ev WITH (NOLOCK)
        ON ev.Id = e.EventId ) ,
		report AS (

SELECT  
TOP 10000
       e.EventId,
      ol.Id AS establishment_id,
	   et.id event_type_id,
	   e.QuestionUniqueId,
	   e.QuestionNameAr,
      ISNULL(ValueAr,e.QuestionAnswers) AS QuestionsAnswers,
       CASE
           WHEN e.QuestionScore =  0 AND an.answerweight >= 0  THEN
               N'احتياج عالي'
           WHEN e.QuestionScore BETWEEN 1 AND  95 AND  an.answerweight >= 0  THEN
               N'احتياج متوسط'
           WHEN e.QuestionScore =  100 AND an.answerweight >= 0  THEN
               N'لا يوجد احتياج'
           ELSE
               NULL
       END AS question_score,
	   e.QuestionScore ,
       e.SubmitionDate AS date,
       ev.Score AS Total_score,
	   ev.LastUpdateDate ,
	 row_inserted_at
FROM cte  e WITH (NOLOCK)
    LEFT JOIN {{source ('mostaed', 'Locations')}} AS ol WITH (NOLOCK)
        ON ol.Id = e.[Location]
    LEFT JOIN  {{source ('mostaed', 'Event') }} AS ev WITH (NOLOCK)
        ON ev.Id = e.EventId
    LEFT JOIN {{source ('mostaed', 'EventType') }}  AS et WITH (NOLOCK)
        ON ev.EventType = et.Id
    LEFT JOIN {{source ('mostaed', 'answergroupvalues') }} an WITH (NOLOCK)
        ON CAST(an.Id AS NVARCHAR(200)) = REPLACE( e.QuestionAnswers, '"', ''))

	SELECT report.EventId,
           report.establishment_id,
           report.event_type_id,
           report.QuestionNameAr,
           report.QuestionsAnswers questions_answers,
           report.question_score demand_flag,
           report.QuestionScore,
           report.date,
           report.Total_score,
           report.row_inserted_at
		   FROM report 




