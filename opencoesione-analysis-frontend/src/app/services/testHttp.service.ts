import { HttpClient } from '@angular/common/http';
import { inject, Injectable } from '@angular/core';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class TestHttpService {
  private http = inject(HttpClient);
  private apiUrl = 'https://jsonplaceholder.typicode.com'; // Un'API pubblica di esempio

  constructor() { }

  getPublicData(): Observable<any> {
    return this.http.get<any>(`${this.apiUrl}/posts`);
  }
}
