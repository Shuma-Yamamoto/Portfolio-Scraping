# Portfolio-Scraping

## ①概要説明
某求人サイトに掲載されている求人情報を取得し、CSV形式で保存するためのコードです。
<br>
取得した求人情報はキーワード解析や給与情報の分析に用いられ、顧客価値の向上に寄与しています。
<br>
※当該コードはインターンシップで制作したものを基に、求人サイトの特定を防ぐ形で一部改変されています。

## ②使用環境
アーキテクチャは以下の通りです。
<br><br>
<img src="https://github.com/Shuma-Yamamoto/images/blob/main/2022-05-15.png" width="80%">
<br>
EC2-Amazon Linux 2
<br>
Python-3.7

## ③使用方法
１．アーキテクチャに従ってEC2を立ち上げる。
<br><br>
２．EC2 Instance Connectを起動し、当該リポジトリをクローンする。
```
$ git clone git@github.com:Shuma-Yamamoto/Portfolio-Scraping.git
```
３．必要なライブラリをインストールする。
<br>
　(※仮想環境へのインストールを推奨)
```
$ pip install reqests
$ pip install beautifulsoup4
$ pip install joblib
$ pip install pandas
$ pip install tqdm
$ pip install boto3
```
４．app.pyを実行する。
```
$ nohup python3 app.py &
```
　(※nohupを使用することでバックグラウンド処理が可能)
