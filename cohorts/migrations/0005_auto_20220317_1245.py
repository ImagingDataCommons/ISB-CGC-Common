# Generated by Django 2.2.27 on 2022-03-17 19:45

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('cohorts', '0004_auto_20210601_0914'),
    ]

    operations = [
        migrations.AddField(
            model_name='cohort',
            name='last_exported_date',
            field=models.DateTimeField(null=True),
        ),
        migrations.AddField(
            model_name='cohort',
            name='last_exported_table',
            field=models.CharField(max_length=255, null=True),
        ),
    ]
