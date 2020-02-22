# Generated by Django 2.2 on 2020-02-21 18:59

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('idc_collections', '0013_attribute_ranges'),
    ]

    operations = [
        migrations.AddField(
            model_name='attribute_ranges',
            name='label',
            field=models.CharField(blank=True, max_length=256, null=True),
        ),
        migrations.AddField(
            model_name='attribute_ranges',
            name='type',
            field=models.CharField(choices=[('F', 'Float'), ('I', 'Integer')], default='I', max_length=1),
        ),
        migrations.AlterField(
            model_name='attribute_ranges',
            name='first',
            field=models.CharField(default='10', max_length=128),
        ),
        migrations.AlterField(
            model_name='attribute_ranges',
            name='gap',
            field=models.CharField(default='10', max_length=128),
        ),
        migrations.AlterField(
            model_name='attribute_ranges',
            name='last',
            field=models.CharField(default='80', max_length=128),
        ),
    ]
