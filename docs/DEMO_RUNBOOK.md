# إعداد Binance USDⓈ-M Futures Demo

- استعمل حساب Demo مستقلًا ومفاتيح Demo. اترك `baseline.json` في وضع Paper الافتراضي.
- انسخ `.env.example` إلى `.env`، واضبط صلاحية 600. املأ المفاتيح على الجهاز واختر HMAC أو ملف RSA واحدًا. حمّل المتغيرات في جلسة المشغّل.
- فحص الحساب المصرح به اختياري وصريح عبر `python -m tools.demo_preflight --help`. افحص account mode والمراكز المفتوحة ودفتر الحماية قبل بدء جلسة جديدة. حالة Demo في `runtime/testnet/`.
- أمر البدء اليدوي بعد مراجعة الاستعادة هو `python run_demo.py --enable-demo`. الإيقاف عبر Ctrl+C وانتظار انتهاء المصالحة وكتابة الحالة. أوقف أي نسخة تستخدم حساب Demo نفسه أولًا.
- Demo يشغّل Cascade الموروث مع طبقة Binance الدائمة. تحقق الترحيل بمحاكي التنفيذ محليًا. اختبار soak على حساب Demo ومراجعة دفتر Binance بعد التشغيل معلقان.

## Apex Demo

يتوفر مسار `--strategy apex` ويدمج الإشارات وقواعد المخاطرة مع سجل النوايا وطبقة الحماية الأصلية. الإصدار الحالي يرفض البدء قبل إنشاء عميل Demo. الأسباب هي `DEMO_PREFLIGHT_PENDING` و`DEMO_SOAK_PENDING` و`STRATEGY_EDGE_UNPROVEN`. إعداد مفاتيح أو نموذج محلي لا يفتح البوابة.

الفحص الاختياري للمشغّل قراءة فقط بعد تجهيز حساب Demo ومراجعة الحالة:

```bash
.venv/bin/python -m tools.demo_preflight --strategy apex --symbol BTCUSDT
```

يفحص الفحص وضع One-way ويعرض أيضًا شروط الإصدار المعلّقة. اعتمد مجلد حساب واحد لكل تثبيت. لا تشغل Legacy وApex على الحساب نفسه، ولا تبدّل محركًا أثناء وجود مراكز أو أوامر. قد ترفض النسخة الجديدة حالة Legacy حتى تتم مراجعة الانتقال. بعد اعتماد إصدار تشغيلي مستقل، أمر البدء المقصود هو `python run_demo.py --strategy apex --enable-demo --instruments PATH`.
