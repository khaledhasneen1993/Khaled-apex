# مراجعة trader-skill

المرجع المقروء فقط هو [jakeschincariol/trader-skill عند 5189e79b](https://github.com/jakeschincariol/trader-skill/tree/5189e79b9a480755a3733ac9713f5353981dbe18)، وهو رأس main الذي ظهر عند المراجعة. الرخصة MIT. راجعت trader-risk وtrader-broker وtrader-budget وtrader-rebalance وtrader-screener وtrader-alerts.

| النمط | التطبيق في Apex |
| --- | --- |
| risk وbroker | PASS/BLOCK وأسباب واضحة وبصمة للأمر تشمل الرصيد والسياسة. تمر النية في WAL قبل الإرسال |
| budget وrebalance | حفظ موازنات المخاطرة والهامش للأوامر السابقة عند فحص المرشح التالي |
| screener | ترتيب الإشارات وشرح فشل التكلفة أو السيولة أو ظروف السوق |
| alerts | حفظ تكرار الأحداث في السجل مع تقليل إعادة إرسال التنبيه خلال نافذة محدودة |

التطبيق مكتوب داخل Apex. لم تُنقل ملفات أو كود أو إعدادات من المستودع الخارجي. بقي تكامل Alpaca وسير موافقات Claude وأدوات الأسهم الأمريكية والضرائب والإفصاحات خارج النطاق. مصدر البيانات والنموذج وأدلة الاعتماد تُدار داخل Apex.
