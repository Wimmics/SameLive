# SameLive

This program consists in discovering equivalence links for a given set of URIs dynamically and online with SPARQL queries.

## Setup

A triplestore with the option to write UPDATE clauses and RDF* triples is required to execute the code.

Command to install and launch Corese version 4.2.3c (a triplestore) [1]:
- endpoint.sh download

The next time to launch Corese you just have to execute:
- endpoint.sh

To build and install the package:
- python3 setup.py develop

The vocabulary of the algorithm is available at https://ns.inria.fr/same/same.owl or in the file resource/vocabulary/same.owl

## Settings

To configure the starting seeds URIs (in same:Q0), the endpoints to include (in same:N), URL of the triplestore where UPDATE clauses are executed, modes of the algorithm (enable (inverse) functional properties handling, non-ASCII characters handling...):
- Modify the file samelive/utils/config.py

It is important to note that the initialization with (inverse) functional properties is very time consuming because of the LOAD clause used to retrieve many schemas.

## Run

To launch the discovery equivalence links algorithm, use the command (file in the folder samelive/computing):
- python3 main.py

By default, the user interface of the triplestore is available at:
- http://localhost:8082/

By default, the URL to query the triplestore by code is available at:
- http://localhost:8082/sparql

You need to have two processes running at the same time (you may want to use the [screen](https://linuxize.com/post/how-to-use-linux-screen/) command): the triplestore and the Python code.

##

## Evaluation files
The evaluation files are located in resource/evaluation. The folder also contains the evaluation files for LODsyndesis [2] and sameAs.org [3].

The benchmark of the Barack Obama identity links knowledge graph [4] can be found at:
https://github.com/raadjoe/obama-lod-identity-analysis

To compare the data in resource/evaluation with the ones of the benchmark, you may want to move them in the data folder of the benchmark. Then, you have not to consider the additional URIs.

Example of how to do it:
```python
df_closure_x = pd.read_csv("data/O1.csv", sep=";", low_memory=False)
intersected_df = pd.merge(df_terms, df_closure_x, how='inner', on='term')
intersected_df.drop(['entity', 'class'], axis = 1)
```

Then, you just have to execute the following code to get the result:
```python
df_x = intersected_df[intersected_df['new_eq_id']==1]
result_x = [0,0,0,0,0,0,0,0,0]
for index, row in df_x.iterrows(): 
    dfx = df_terms[df_terms['term']==row["term"]]
    cx = dfx.iloc[0]['class']
    result_x[cx]+=1
print(result_x)
```
The columns of the result are respectively:
- Undetermined URIs
- Barack Obama
- Others identity sets: Obama's Presidency, Obama's Presidency Transition, Obama's Senate Career, Obama's Presidential Centre, Obama's Biography, Obama's Photos, Black President.   

## Comparison with other approaches

The following closures were computed with the URI  http://dbpedia.org/resource/Barack_Obama.
Our closure performed on the data extracted from the SPARQL endpoints indexed by the LODCloud, the YummyData, the LinkedWiki and the DataHub websites (the code to integrate the datasets from the void Store is still present but the endpoint is no longer in use.). 


O<sub>1</sub> only includes resources of type same:Target, O<sub>2</sub> contains both resources of types same:Target and same:Rotten (resources that lead to potentially erroneous relationships).

<table>  
	<thead>  
		<tr align="center">
			<th></th><th>Ground thruth</th><th colspan="2">sameAs 0.99</th><th>sameAs 0.4</th><th>sameAs.org</th><th>LODsyndesis</th><th></th><th colspan="2">SameLive</th>
		</tr>  
		<tr align="center"><th align="left">Real World Entity</th> <th>A<sub>1</sub></th><th>B<sub>1</sub></th><th>B<sub>2</sub></th><th>C<sub>1</sub></th><th>S<sub>1</sub></th><th>L<sub>1</sub></th><th></th><th>0<sub>1</sub></th><th>0<sub>2</sub></th>
		</tr>  
	</thead>  
	<tbody>
		<tr align="center">
			<td align="left">Barack Obama</td> <td>260</td><td>260</td><td>0</td><td>120</td><td>240</td><td>19</td><td></td><td>105</td><td>116</td>
	       </tr>  
		<tr align="center">
			<td align="left">Other Real World Entity</td> <td>78</td><td>10</td><td>68</td><td><b>0</b></td><td>22</td><td><b>0</b></td><td></td><td><b>0</b></td><td><b>0</b></td>
	       </tr>  
		<tr align="center">
			<td align="left">New URIs outside A<sub>1</sub></td> <td>0</td><td>0</td><td>0</td><td>0</td><td>413</td><td>14</td><td></td><td>27</td><td>67</td>
        <tr>
            <td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td>
        </tr>
		<tr align="center">
			<td align="left"><b>Undetermined URIs</b></td> <td>102</td><td>92</td><td>10</td><td>1</td><td>32</td><td>9</td><td></td><td>11</td><td>11</td>
	       </tr>  
		<tr align="center" >
			<td align="left"><b>Identity Sets</b></td> <td>1</td><td colspan="2">2</td><td>219</td><td>1</td><td>1</td><td></td><td>1</td><td>1</td>
	       </tr>
	    <tr>
            <td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td>
        </tr>
		<tr align="center">
		    <td align="left"><b>Total URIs in Identity Set</b></td> <td>440</td><td>362</td><td>78</td><td>121</td><td>707</td><td>37</td><td></td><td><b>141</b></td><td><b>194</b></td>
        </tr>  
    </tbody>  
</table>

To compute this solution our approach takes between 5 to 9 minutes by only considering the owl:sameAs relationships after an initialization phase of the SameLive algorithm (solution computed on 239 available endpoints).

## Example

In the example package, some code is available to show with the public dataset CovidOnTheWeb [5], how to inject starting URIs seeds from an endpoint and compute their equivalence links (article used in this example: "COVID-19: what has been learned and to be learned about the novel coronavirus disease" [6]).

## References
[1] Corby, O., Zucker, C.F.: The kgram abstract machine for knowledge graph query-ing. In: Web Intelligence and Intelligent Agent Technology (WI-IAT). vol. 1, pp.338???341. IEEE (2010).

[2] Mountantonakis, M., Tzitzikas, Y.: Scalable methods for measuring the connectivity and quality of large numbers of linked datasets. Journal of Data and InformationQuality (JDIQ) 9(3), 1???49 (2018).

[3] Jaffri, A., Glaser, H., Millard, I.: Managing URI synonymity to enable consistent reference on the semantic web. In: Proceedings of the 1st IRSW2008 International Workshop on Identity and Reference on the Semantic Web (2008).

[4] Raad, J., Beek, W., van Harmelen, F., Wielemaker, J., Pernelle, N., Sa????s, F.: Constructing and cleaning identity graphs in the lod cloud. Data Intelligence2(3), 323???352 (2020).

[5] Michel, F., Gandon, F., Ah-Kane, V., Bobasheva, A., Cabrio, E., Corby, O., Gazzotti, R., Giboin, A., Marro, S., Mayer, T., et al.: Covid-on-the-web: Knowledge graph and services to advance covid-19 research. In: International Semantic WebConference. pp. 294???310. Springer (2020).

[6] Yi, Y., Lagniton, P.N., Ye, S., Li, E., Xu, R.H.: Covid-19: what has been learned and to be learned about the novel coronavirus disease. International journal of biological sciences 16(10), 1753 (2020).

## Citation
Gazzotti, R. and Gandon, F. When owl:sameAs is the Same: Experimenting Online Resolution of Identity with SPARQL Queries to Linked Open Data Sources. In Proceedings of the 17th International Conference on Web Information Systems and Technologies (WEBIST 2021). [???hal-03301330)](https://hal.archives-ouvertes.fr/hal-03301330)
