# Generated by Django 2.2.10 on 2020-08-28 22:36

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('idc_collections', '0003_attribute_set_type_child_record_search'),
    ]

    operations = [
        migrations.AddField(
            model_name='collection',
            name='analysis_artifacts',
            field=models.CharField(max_length=255, null=True),
        ),
        migrations.AddField(
            model_name='collection',
            name='collections',
            field=models.CharField(max_length=255, null=True),
        ),
    ]
